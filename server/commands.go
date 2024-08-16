package main

import (
	"errors"
	"fmt"
)

var (
	ErrNoDbSelected     = errors.New("no db is selected")
	ErrNotAuthorized    = errors.New("not authorized")
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrKeyNotExists     = errors.New("key not exists")
	ErrArgumentCount    = errors.New("unexpected amount of arguments")
	ErrAuthLoginFailed  = errors.New("authentication failed")
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

	key := args[0]
	value := args[1]

	if _, ok := s.databases[con.db].data[key]; ok {
		return "", errors.New("Key doesn't exists")
	}

	s.databases[con.db].data[key] = Record{Value: value, Ttl: -1}

	// by design we're returning the value that is stored in the dictionary
	// to confirm that assignment was done correctly. The client could use that.
	return s.databases[con.db].data[key].Value, nil
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
