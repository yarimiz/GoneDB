package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

var commandsMap = map[byte]func([]string, *TcpServer, *Connection) (string, error){
	0x01: Ping,
	0x02: Set,
	0x03: Get,
	0x04: Replace,
	0x05: UpdateTtl,
	0x06: NotImplemented,
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
	ErrInvalidArgument  = errors.New("invalid argument")
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

func validateLoggedIn(con *Connection) error {
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

	err = validateLoggedIn(con)
	if err != nil {
		return "", err
	}

	err = validateDbSelected(con)
	if err != nil {
		return "", err
	}

	key := args[0]
	if rec, ok := s.databases[con.db].data[key]; ok {

		if rec.Ttl() < 0 {
			// record is expired, delete it and return error key not found
			delete(s.databases[con.db].data, key)

			return "", ErrKeyNotExists
		}

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
	err := validateLoggedIn(con)
	if err != nil {
		return "", err
	}

	return con.user.username, nil
}

func NotImplemented(args []string, s *TcpServer, con *Connection) (string, error) {
	return "NOT IMPLEMENTED YET", nil
}

func UpdateTtl(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 2)
	if err != nil {
		return "", err
	}

	err = validateLoggedIn(con)
	if err != nil {
		return "", err
	}

	err = validateDbSelected(con)
	if err != nil {
		return "", err
	}

	key := args[0]
	ttl_parsed, err := strconv.Atoi(args[1])

	if err != nil {
		return "", ErrInvalidArgument
	}
	ttl := int64(ttl_parsed)

	if ttl < 0 {
		return "", errors.New("ttl must be a positive integer")
	}

	if _, ok := s.databases[con.db].data[key]; !ok {
		return "", ErrKeyNotExists
	}

	obj := s.databases[con.db].data[key]
	obj.Expiry = time.Now().Unix() + ttl

	s.databases[con.db].data[key] = obj

	return "OK", nil
}

func Set(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 2)
	if err != nil {
		return "", err
	}

	err = validateLoggedIn(con)
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

	s.databases[con.db].data[key] = Record{Value: value, Expiry: -1}

	// by design we're returning the value that is stored in the dictionary
	// to confirm that assignment was done correctly. The client could use that.
	return s.databases[con.db].data[key].Value, nil
}

func SelectDb(args []string, s *TcpServer, con *Connection) (string, error) {
	err := validateArgsCount(args, 1)
	if err != nil {
		return "", err
	}

	err = validateLoggedIn(con)
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

	err = validateLoggedIn(con)
	if err != nil {
		return "", err
	}

	key := args[0]

	if _, ok := s.databases[con.db].data[key]; !ok {
		return "", ErrKeyNotExists
	}

	value := args[1]
	record := s.databases[con.db].data[key]
	record.Value = string(value)
	s.databases[con.db].data[key] = record

	// by design we're returning the value that is stored in the dictionary
	// to confirm that assignment was done correctly. The client could use that.
	return s.databases[con.db].data[key].Value, nil
}

func Disconnect(args []string, s *TcpServer, con *Connection) (string, error) {
	// TODO implement
	return "", nil
}
