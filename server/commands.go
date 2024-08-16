package main

import (
	"errors"
	"fmt"
)

func get(args []string, s *TcpServer) (string, error) {
	if len(args) != 1 {
		return "", errors.New("unexpected amount of arguments")
	}

	key := args[0]
	if rec, ok := s.data[key]; ok {
		return rec.Value, nil
	} else {
		return "", fmt.Errorf("key `%s` not exists", key)
	}
}

func ping(args []string, s *TcpServer) (string, error) {
	return "PONG", nil
}

func set(args []string, s *TcpServer) (string, error) {
	if len(args) != 2 {
		return "", errors.New("Invalid number of args")
	}

	key := args[0]
	value := args[1]

	if _, ok := s.data[key]; ok {
		return "", errors.New("Key doesn't exists")
	}

	s.data[key] = Record{Value: value, Ttl: -1}

	// by design we're returning the value that is stored in the dictionary
	// to confirm that assignment was done correctly. The client could use that.
	return s.data[key].Value, nil
}

func replace(args []string, s *TcpServer) (string, error) {
	if len(args) != 2 {
		return "", errors.New("Invalid number of args")
	}

	key := args[0]

	if _, ok := s.data[key]; ok {
		value := args[1]
		s.data[string(key)] = Record{Value: string(value), Ttl: -1}

		// by design we're returning the value that is stored in the dictionary
		// to confirm that assignment was done correctly. The client could use that.
		return s.data[string(key)].Value, nil

	} else {
		return "", errors.New("Key doesn't exists")
	}
}

func disconnect(args []string, s *TcpServer) (string, error) {
	// TODO implement
	return "", nil
}
