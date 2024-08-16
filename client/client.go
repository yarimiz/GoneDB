package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:1234")

	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	fmt.Println("Connected successfully")

	for {
		reader := bufio.NewReader(os.Stdin)
		text, err := reader.ReadBytes('\n')

		if err != nil {
			fmt.Printf("Failed reading input as bytes")
		}

		p, err := encodePackage(text)
		if err != nil {
			fmt.Printf("Failed creating package")
		}

		conn.Write(p)

		buf := make([]byte, 1024)
		_, err = conn.Read(buf)

		if err != nil {
			fmt.Printf("Failed reading bytes from server")
		}

		fmt.Print(string(buf))
	}
}

func encodePackage(input []byte) ([]byte, error) {
	payload := [3]byte{}

	// set the version as 1
	payload[0] = 1

	re := regexp.MustCompile(`(?m)^(?P<Command>[A-Z]+)(?:\s+(?P<Args>[^|\n]*))?(?:\s*\|\s*(?P<Pairs>(?:\s*[a-zA-Z]+\s+\d+\s*)*))?$`)
	match := re.FindStringSubmatch(string(input))

	if len(match) == 0 {
		return []byte{}, errors.New("input is missing a command")
	}

	cmd_str := match[re.SubexpIndex("Command")]
	cmd_args := match[re.SubexpIndex("Args")]

	length := len(cmd_args)

	command, err := encodeCommand(cmd_str)
	if err != nil {
		log.Fatalf("unknown command: %s", cmd_str)
		return []byte{}, err
	}
	payload[1] = command
	payload[2] = byte(length)

	result := append(payload[:], []byte(cmd_args)...)

	return result, nil
}

func encodeCommand(command string) (byte, error) {

	if cmdByte, exists := commandMap[command]; exists {
		return cmdByte, nil
	}

	return 0x00, fmt.Errorf("unknown command: %s", command)
}

var commandMap = map[string]byte{
	"PING":       0x01,
	"SET":        0x02,
	"GET":        0x03,
	"REPLACE":    0x04,
	"DB":         0x40,
	"LOGIN":      0x50,
	"WHOAMI":     0x51,
	"DISCONNECT": 0x99,
}
