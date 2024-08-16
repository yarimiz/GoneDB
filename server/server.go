package main

import (
	"bufio"
	"bytes"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
)

type TcpServer struct {
	listener  net.Listener
	quitch    chan struct{}
	databases map[int8]Database
	users     map[string]User
}

type User struct {
	username string
	pass     string
	perms    map[int8]byte
}

type Database struct {
	data map[string]Record
}

type Connection struct {
	raw           net.Conn
	db            int8
	authenticated bool
	user          User
}

type Record struct {
	Value string
	Ttl   int32
}

var server *TcpServer

func newConnection(raw net.Conn) *Connection {
	return &Connection{raw: raw, db: -1}
}

func handler_hello_world(w http.ResponseWriter, r *http.Request) {
	fp := path.Join("templates", "index.html")
	tmpl, err := template.ParseFiles(fp)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tmpl.Execute(w, server.databases); err != nil {
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
		raw_conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		conn := newConnection(raw_conn)

		go s.readLoop(conn)
	}
}

func (s *TcpServer) readLoop(conn *Connection) {
	defer conn.raw.Close()

	for {
		buf := make([]byte, 1024)

		_, err := conn.raw.Read(buf)

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
			conn.raw.Write([]byte(err.Error() + "\r\n"))
			continue
		}

		if cmdFunc, exists := commandsMap[buf[1]]; exists {
			resp, err := cmdFunc(args, s, conn)

			if err != nil {
				// for now we're just returning the error text like a regular message
				resp = "ERROR: " + err.Error()
			}

			conn.raw.Write([]byte(resp + "\r\n"))
		} else {
			conn.raw.Write([]byte("ERROR invalid command\r\n"))
		}
	}
}

func main() {

	startHttpServer()

	server = newTcpServer()
	server.LoadUsers()
	server.Start()
}

func startHttpServer() {
	http.HandleFunc("/", handler_hello_world)
	go http.ListenAndServe(":8080", nil)
	fmt.Println("HTTP server is listening on 8080....")
}

func (s *TcpServer) LoadUsers() map[int8]User {

	file, err := os.Open("users.acl")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	users := make(map[int8]User)

	for scanner.Scan() {
		line_txt := scanner.Text()

		if len(line_txt) == 0 || line_txt[0] == byte('#') {
			continue
		}

		parts := strings.Split(scanner.Text(), ":")

		usr_name := parts[0]
		usr_pass := parts[1]

		usr := User{username: usr_name, pass: usr_pass, perms: map[int8]byte{}}

		for _, p := range strings.Split(parts[2], " ") {
			pp := strings.Split(p, ",")
			db_id, err := strconv.Atoi(pp[0])

			if err != nil {
				panic(err)
			}

			db_perm, err := strconv.Atoi((pp[1]))

			if err != nil {
				panic(err)
			}

			usr.perms[int8(db_id)] = byte(db_perm)
		}

		server.users[usr.username] = usr

	}

	return users
}

func newTcpServer() *TcpServer {

	return &TcpServer{
		quitch:    make(chan struct{}),
		databases: make(map[int8]Database),
		users:     make(map[string]User),
	}
}
