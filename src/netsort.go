package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type recordedList struct {
	k []byte
	v []byte
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

/**
func conToServer(serverId int, toConnect [16]net.Conn, scs ServerConfigs) {
	//fmt.Println("Debug to see if it goes to listenToData functionn")

	//counter := 0
	size := len(scs.Servers)
	//fmt.Println("The size is", size)

}
*/

/**
 * The purpose of this function is to convert string to binary
 */
func StringTobinary(str string) string {
	result := ""
	for _, v := range str {
		result = fmt.Sprintf("%s%.8b", result, v)
	}
	return result
}

/**
 * The purpose of this function is to convert binary to integer
 */
func binaryToInt(s string) int64 {
	i, _ := strconv.ParseInt(s, 2, 64)
	return i
}

func listenToData(channel chan<- []byte, conType string, host string, port string, scs ServerConfigs) {

	// I wrote based on discussion code that TA explained
	//fmt.Println("Listen to sunction has been entered")
	listenTo, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}

	defer listenTo.Close()

	for {
		conn, err := listenTo.Accept()
		fmt.Println("ACCEPTED")
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
			return
		}
		//fmt.Println("MASHALA")
		go handelCon(conn, channel)
	}
}

// The handel function I wrote based on discussion code that TA explained
func handelCon(connecting net.Conn, channel chan<- []byte) {

	//fmt.Println("Handel function enter to function as I desire")
	for {

		buffering := make([]byte, 101)
		num, err := connecting.Read(buffering)
		if err == nil {
			sending := buffering[0:num]
			channel <- sending
			output_Stream := int(sending[0])
			if output_Stream == 1 {
				connecting.Close()
				break
			}
		} else {
			log.Fatal("Something goes wrong")
		}
	}

}

func sendDataOfClient(bitServer int, server int, toConnect [16]net.Conn) {
	//fmt.Println("sendDataOfClient function enter to function as I desire")
	fileInfo, err := os.Open(os.Args[2])
	checkError(err)
	defer fileInfo.Close()
	file := bufio.NewReader(fileInfo)

	for {
		chunk := make([]byte, 100)
		_, err := io.ReadFull(file, chunk)
		if err != nil {

			if err == io.ErrUnexpectedEOF || err == io.EOF {

				break
			}

			fmt.Printf("The is an Error for %s: ", err)
		}

		// Get on byte since we need at most 4 bit
		sliceOf := chunk[0]

		// Based on what TA said on piazza, we use shift from CSE30
		switch bitServer {
		case 1:
			sliceOf = sliceOf >> 7
		case 2:
			sliceOf = sliceOf >> 6
		case 3:
			sliceOf = sliceOf >> 5
		case 4:
			sliceOf = sliceOf >> 4
		}

		convert := int(sliceOf)

		if convert == server {
			continue
		} else {
			//defer allconection[idServer].connection.Close()
			char := []byte{0}
			char = append(char, chunk...)
			toConnect[convert].Write(char)
		}
	}

	//Here is the point that we are at the end of our file
	// So we need to let the server know that we have no more data to read
	chunk2 := make([]byte, 100)
	finalChunk := []byte{1}
	finalChunk = append(finalChunk, chunk2...)
	for i := 0; i < len(toConnect); i++ {
		if i != server {
			if toConnect[i] != nil {
				toConnect[i].Write(finalChunk)
			}
		} else {
			continue
		}

	}

	// Now close the files since we put 1000000 inside it

	for i := 0; i < len(toConnect); i++ {

		if i != server {
			if toConnect[i] != nil {
				defer toConnect[i].Close()
			}
		} else {
			continue
		}

	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/
	//sizeOfServer := len(scs.Servers)
	//serverCheck
	// List of what we wanna connect to
	sizeOfsever := len(scs.Servers)
	var toConnect [16]net.Conn
	/////////////////////////////////////////////////
	//       fmt.Printf("%f", temp)                /
	//       fmt.Printf("%d",bitServer)           /
	//////////////////////////////////////////////
	/** Since we have 2^n server, we need to use N bits to determine
	 *  which server we suppose to refer to
	 * So, we need to get the first N bit of the sizeServer
	 */

	temp := math.Log2(float64(sizeOfsever))
	bitServer := int(temp)
	channel := make(chan []byte)
	defer close(channel)
	endOf := sizeOfsever - 1
	reaching := 0
	//Let's listen ti data to open the port
	go listenToData(channel, "tcp", scs.Servers[serverId].Host, scs.Servers[serverId].Port, scs)

	//Let's try to contact other servers
	for i := 0; i < sizeOfsever; i++ {
		if i != serverId {
			for {

				connections, err := net.Dial("tcp", scs.Servers[i].Host+":"+scs.Servers[i].Port)
				if err == nil {
					//defer connections.Close()
					toConnect[i] = connections
					fmt.Println("DIALING", serverId, "dialing server ", i)
					break
				} else {
					continue
				}
			}

		} else {
			continue
		}
	}
	//Here we send data from dialing
	go sendDataOfClient(bitServer, serverId, toConnect)

	// Now let's write out function to output file
	var group []recordedList

	// Basically I will use part of writing from my project 1 code to this project and more things
	for {

		if reaching != endOf {
			data := <-channel
			getFirstByte := int(data[0])

			if getFirstByte != 1 {
				key := data[1:11]
				value := data[11:]
				var result *recordedList = new(recordedList)
				result.k = key
				result.v = value
				group = append(group, *result)
			} else {
				reaching++
				continue
			}

		} else {
			break
		}

	}

	fileInfo, err := os.Open(os.Args[2])
	checkError(err)
	defer fileInfo.Close()
	filed := bufio.NewReader(fileInfo)

	for {
		chunk := make([]byte, 100)
		_, errA := io.ReadFull(filed, chunk)

		if errA != nil {

			if errA == io.ErrUnexpectedEOF || errA == io.EOF {

				break
			}

			fmt.Printf("The is an Error for %s: ", errA)
		}

		keys := chunk[:10]
		vals := chunk[10:]
		oneByte := keys[0]
		switch bitServer {
		case 1:
			oneByte = oneByte >> 7
		case 2:
			oneByte = oneByte >> 6
		case 3:
			oneByte = oneByte >> 5
		case 4:
			oneByte = oneByte >> 4
		}

		conV := int(oneByte)
		var ress *recordedList = new(recordedList)
		if conV == serverId {
			ress.k = keys
			ress.v = vals
			group = append(group, *ress)
		}
	}

	//Time to sort our data before writing to output file
	sort.SliceStable(group, func(i, j int) bool {

		ressult := bytes.Compare(group[i].k, group[j].k)

		if ressult < 0 {
			return true
		} else {
			return false
		}
	})

	// Let's write finally
	outFile, errB := os.Create(os.Args[3])
	checkError(errB)
	defer outFile.Close()

	leg := len(group)
	for i := 0; i < leg; i++ {

		_, errK := outFile.Write(group[i].k)
		checkError(errK)
		_, errV := outFile.Write(group[i].v)

		checkError(errV)
	}

}
