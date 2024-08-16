package main

import (
	"errors"
	"fmt"
	"strconv"
)

var commandsMap = map[byte]func([]string, *TcpServer, *Connection) (string, error){
	0x01: Ping,
	0x02: Set,
	0x03: Get,
	0x04: Replace,
	0x40: SelectDb,
	0x50: AuthLogin,
	0x51: AuthWhoAmI,
	0x99: Disconnect,
}

var (
	ErrNoDbSelected     = errors.New("no db is selected")
	ErrNotAuthorized    = errors.New("not authorized")
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrKeyNotExists     = errors.New("key not exists")
	ErrArgumentCount    = errors.New("unexpected amount of arguments")
	ErrAuthLoginFailed  = errors.New("authentication failed")
	ErrDbNotExists      = errors.New("db not exists")
	ErrDbIdParseFailed  = errors.New("db id parsing failed")
)

const (
	PermRead      = 0x01
	PermReadWrite = 0x02
)

func validateDbSelected(con *Connection) error {
	if con.db == -1 {
		return ErrNoDbSelected
	}

	if _, exists := con.user.perms[con.db]; !exists {
		return ErrNotAuthorized
	}

	return nil
}

func validateAuthorization(con *Connection) error {
	if con.user.username == "" {
		return ErrNotAuthorized
	}

	return nil
}

func validateArgsCount(args []string, n int) error {
	if len(args) != n {
		return errors.New("unexpected amount of arguments")
	}

	return nil
}

func Get(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 1)
	if err != nil {
		return "", err
	}

	err = validateAuthorization(con)
	if err != nil {
		return "", err
	}

	err = validateDbSelected(con)
	if err != nil {
		return "", err
	}

	key := args[0]
	if rec, ok := s.databases[con.db].data[key]; ok {
		return rec.Value, nil
	} else {
		return "", ErrKeyNotExists
	}
}

func Ping(args []string, s *TcpServer, con *Connection) (string, error) {
	return "PONG", nil
}

func AuthLogin(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 2)
	if err != nil {
		return "", err
	}

	fmt.Println(args[0])
	fmt.Println(args[1])

	if usr, exists := s.users[args[0]]; exists {
		if usr.pass == args[1] {
			con.user = usr
			return usr.username, nil
		}
	}

	return "", ErrAuthLoginFailed
}

func AuthWhoAmI(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateAuthorization(con)
	if err != nil {
		return "", err
	}

	return con.user.username, nil
}

func NotImplemented(args []string, s *TcpServer, con *Connection) (string, error) {
	return "PONG", nil
}

func Set(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 2)
	if err != nil {
		return "", err
	}

	err = validateAuthorization(con)
	if err != nil {
		return "", err
	}

	err = validateDbSelected(con)
	if err != nil {
		return "", err
	}

	key := args[0]
	value := args[1]

	if _, ok := s.databases[con.db].data[key]; ok {
		return "", ErrKeyNotExists
	}

	s.databases[con.db].data[key] = Record{Value: value, Ttl: -1}

	// by design we're returning the value that is stored in the dictionary
	// to confirm that assignment was done correctly. The client could use that.
	return s.databases[con.db].data[key].Value, nil
}

func SelectDb(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 1)
	if err != nil {
		return "", err
	}

	err = validateAuthorization(con)
	if err != nil {
		return "", err
	}

	db_id_int64, err := strconv.ParseInt(args[0], 10, 8)
	if err != nil {
		return "", ErrDbIdParseFailed
	}

	db_id := int8(db_id_int64)
	if _, exists := s.databases[db_id]; !exists {
		// if the db not exists, we just create it.
		s.databases[db_id] = Database{data: make(map[string]Record)}
	}

	if _, exists := con.user.perms[db_id]; !exists {
		return "", ErrNotAuthorized
	}

	con.db = db_id
	return string(db_id), nil
}

func Replace(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 2)
	if err != nil {
		return "", err
	}

	err = validateAuthorization(con)
	if err != nil {
		return "", err
	}

	key := args[0]

	if _, ok := s.databases[con.db].data[key]; ok {
		value := args[1]
		s.databases[con.db].data[key] = Record{Value: string(value), Ttl: -1}

		// by design we're returning the value that is stored in the dictionary
		// to confirm that assignment was done correctly. The client could use that.
		return s.databases[con.db].data[key].Value, nil

	} else {
		return "", errors.New("Key doesn't exists")
	}
}

func Disconnect(args []string, s *TcpServer, con *Connection) (string, error) {
	// TODO implement
	return "", nil
}
