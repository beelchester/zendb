package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_PORT = 6379

// Commands
const (
	PING = "PING"
	ECHO = "ECHO"
	SET  = "SET"
	GET  = "GET"
)

// Returns
const (
	PONG = "PONG"
	OK   = "OK"
)

const NULLBULKSTRING = "$-1\r\n"

type Config struct {
	lnAddr string
}

type Server struct {
	cfg         Config
	ln          net.Listener
	peers       map[*Peer]struct{}
	addPeerChan chan *Peer
	delPeerChan chan *Peer
	msgChan     chan Message
	db          map[string]Data
}

type Peer struct {
	conn           net.Conn
	msgChan        chan Message
	deletePeerChan chan *Peer
	db             *map[string]Data
}

type Data struct {
	value string
	px    int
}

type Message struct {
	cmd  []byte
	peer Peer
}

func NewMessage(peer Peer, msg []byte) Message {
	return Message{
		cmd:  msg,
		peer: peer,
	}
}

func NewPeer(conn net.Conn, msgChan chan Message, delete chan *Peer, db *map[string]Data) *Peer {
	return &Peer{
		conn:           conn,
		msgChan:        msgChan,
		deletePeerChan: delete,
		db:             db,
	}
}

func (p *Peer) reader() {
	buf := make([]byte, 1024)
	for {
		defer func() {
			p.deletePeerChan <- p
			p.conn.Close()
		}()
		n, err := p.conn.Read(buf)
		if err != nil {
			return
		}
		msgBuf := make([]byte, n)
		copy(msgBuf, buf[:n])
		p.msgChan <- NewMessage(*p, msgBuf)
	}
}

func NewServer(c Config) *Server {
	if len(c.lnAddr) == 0 {
		c.lnAddr = fmt.Sprintf(":%d", DEFAULT_PORT)
	}
	return &Server{
		cfg:         c,
		peers:       make(map[*Peer]struct{}),
		addPeerChan: make(chan *Peer),
		delPeerChan: make(chan *Peer),
		msgChan:     make(chan Message),
		db:          make(map[string]Data),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.lnAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	go s.handlePeersLoop()
	return s.AcceptLoop()
}

func (s *Server) handlePeersLoop() {
	for {
		select {
		case peer := <-s.addPeerChan:
			s.peers[peer] = struct{}{}
		case msg := <-s.msgChan:
			HandleMessage(msg)
		case peer := <-s.delPeerChan:
			delete(s.peers, peer)
		}
	}
}

func HandleMessage(msg Message) {
	splits := strings.Split(string(msg.cmd), "\r\n")
	inputBulkStrings := []string{}
	// commandLen := "0"
	// Command Parser
	for i, v := range splits {
		if v == "" {
			continue
		}
		if strings.Contains(v, "*") {
			// commandLen = strings.Split(v, "*")[1]
			// println("len", commandLen)
		}
		if strings.Contains(v, "$") {
			inputBulkStrings = append(inputBulkStrings, splits[i+1])
		}
	}

	// Command Handler
	for i, v := range inputBulkStrings {
		switch strings.ToUpper(v) {
		case PING:
			msg.peer.conn.Write([]byte(bulkString(PONG)))
		case ECHO:
			if i+1 < len(inputBulkStrings) {
				echoReturn := inputBulkStrings[i+1]
				msg.peer.conn.Write([]byte(bulkString(echoReturn)))
			} else {
				msg.peer.conn.Write([]byte(bulkString("ECHO Command requires a string argument")))
			}
		case SET:
			px := -1
			if i+4 < len(inputBulkStrings) {
				if inputBulkStrings[i+3] == "px" {
					x, err := strconv.Atoi(inputBulkStrings[i+4])
					if err != nil {
						msg.peer.conn.Write([]byte(bulkString("Invalid px value")))
					} else {
						px = x
					}
				}
			}
			if i+2 < len(inputBulkStrings) {
				key := inputBulkStrings[i+1]
				value := inputBulkStrings[i+2]
				(*msg.peer.db)[key] = Data{value: value, px: px}
				msg.peer.conn.Write([]byte(bulkString(OK)))
			} else {
				msg.peer.conn.Write([]byte(bulkString("SET Command requires a key and value argument")))
			}
			if px > 0 {
				go func() {
					time.Sleep(time.Duration(px) * time.Millisecond)
					delete((*msg.peer.db), inputBulkStrings[i+1])
				}()
			}
		case GET:
			if i+1 < len(inputBulkStrings) {
				key := inputBulkStrings[i+1]
				value, ok := (*msg.peer.db)[key]
				if ok {
					msg.peer.conn.Write([]byte(bulkString(value.value)))
				} else {
					msg.peer.conn.Write([]byte(NULLBULKSTRING))
				}
			} else {
				msg.peer.conn.Write([]byte(bulkString("GET Command requires a key argument")))
			}

			// default:
			// 	msg.peer.conn.Write([]byte(bulkString("Unknown Command")))
		}
	}
}

func bulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func (s *Server) AcceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	peer := NewPeer(conn, s.msgChan, s.delPeerChan, &s.db)
	s.addPeerChan <- peer
	peer.reader()
}

func main() {
	port := flag.Int("port", DEFAULT_PORT, "port to listen on")
	flag.Parse()
	print(fmt.Sprintf("Running on port: %d\n", *port))
	s := NewServer(Config{lnAddr: fmt.Sprintf(":%d", *port)})
	if err := s.Start(); err != nil {
		slog.Error("Error starting server: %v", err)
	}
}
