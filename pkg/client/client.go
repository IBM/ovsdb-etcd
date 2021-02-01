package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc/jsonrpc"
	"os"
)

func main() {
	client, err := jsonrpc.Dial("tcp", "0.0.0.0:12345")
	//Only change this
	if err != nil {
		log.Fatal(err)
	}
	in := bufio.NewReader(os.Stdin)
	for {
		_, _, err := in.ReadLine()
		if err != nil {
			log.Fatal(err)
		}
		var reply *string
		fmt.Println("Client")
		err = client.Call("monitor_cond", "[\"_Server\",[\"monid\",\"_Server\"],{\"Database\":[{\"columns\":[\"cid\",\"connected\",\"index\",\"leader\",\"model\",\"name\",\"schema\",\"sid\"]}]}]", &reply)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Reply: %v", *reply)
		fmt.Printf("Reply: %v\n", *reply)
	}
}
