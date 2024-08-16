package main

import (
	"bytes"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
)

type TcpServer struct {
	listener net.Listener
	quitch   chan struct{}
	data     map[string]Record
}

type Record struct {
	Value string
	Ttl   int32
}

var server *TcpServer

func handler_hello_world(w http.ResponseWriter, r *http.Request) {
	fp := path.Join("templates", "index.html")
	tmpl, err := template.ParseFiles(fp)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tmpl.Execute(w, server.data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *TcpServer) Start() {
	// TCP SERVER FOR API
	l, err := net.Listen("tcp", "0.0.0.0:1234")
	fmt.Println("TCP is listening on 1234....")
	if err != nil {
		println("failed listening")
		os.Exit(1)
	}

	defer l.Close()
	s.listener = l

	go s.acceptLoop()

	<-s.quitch

}

func (s *TcpServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go s.readLoop(conn)
	}
}

func (s *TcpServer) readLoop(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)

		// we're using telnet which adds \r\n at the end of the command
		// so we remove it because it screws up everything.
		// In any case, we should not support multiline input
		buf, _, _ = bytes.Cut(buf, []byte("\r\n"))

		if err != nil {
			println("failed reading")
			return
		}

		ver := int(buf[0])

		if ver != 1 {
			// should return an error to the client and not handle the message
		}

		length := int(buf[2])
		args := []string{}

		if length > 0 {
			args = strings.Split(string(buf[3:3+length]), " ")
		}

		if err != nil {
			conn.Write([]byte(err.Error() + "\r\n"))
			continue
		}

		if cmdFunc, exists := commandsMap[buf[1]]; exists {
			resp, err := cmdFunc(args, s)

			if err != nil {
				// for now we're just returning the error text like a regular message
				resp = "ERROR: " + err.Error()
			}

			conn.Write([]byte(resp + "\r\n"))
		} else {
			conn.Write([]byte("ERROR invalid command\r\n"))
		}
	}
}

func main() {

	startHttpServer()

	server = newTcpServer()
	server.Start()
}

func startHttpServer() {
	http.HandleFunc("/", handler_hello_world)
	go http.ListenAndServe(":8080", nil)
	fmt.Println("HTTP server is listening on 8080....")
}

func newTcpServer() *TcpServer {
	return &TcpServer{
		quitch: make(chan struct{}),
		data:   make(map[string]Record),
	}
}

var commandsMap = map[byte]func([]string, *TcpServer) (string, error){
	0x01: ping,
	0x02: set,
	0x03: get,
	0x04: replace,
	0x99: disconnect,
}
