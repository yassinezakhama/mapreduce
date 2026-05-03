package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args, reply := GetTaskArgs{}, GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}

		switch reply.Type {
		case Map:
			if handleMap(mapf, reply.TaskID, reply.Filename, reply.NReduce) {
				if !report(reply.TaskID, Map, true) {
					return
				}
			} else if !report(reply.TaskID, Map, false) {
				return
			}

		case Reduce:
			if handleReduce(reducef, reply.TaskID, reply.NMap) {
				if !report(reply.TaskID, Reduce, true) {
					return
				}
			} else if !report(reply.TaskID, Reduce, false) {
				return
			}

		case Wait:
			time.Sleep(time.Second)

		case Exit:
			return

		default:
			log.Fatalf("unexpected mr.TaskType: %#v", reply.Type)
		}
	}
}

func report(taskid int, tasktype TaskType, success bool) bool {
	reportargs := ReportArgs{
		TaskID:   taskid,
		TaskType: tasktype,
		Success:  success,
	}
	reportreply := ReportReply{}
	return call("Coordinator.Report", &reportargs, &reportreply)
}

func handleMap(
	mapf func(string, string) []KeyValue,
	taskid int, filename string, nreduce int) bool {

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open '%v'", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read '%v'", filename)
		return false
	}
	_ = file.Close()

	intermediate := mapf(filename, string(content))

	grouped := make([][]KeyValue, nreduce)
	for _, kv := range intermediate {
		reduceID := ihash(kv.Key) % nreduce
		grouped[reduceID] = append(grouped[reduceID], kv)
	}

	for reduceID, bucket := range grouped {
		interfile, err := ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			log.Printf("cannot create intermediate file")
			return false
		}
		tmpname := interfile.Name()

		enc := json.NewEncoder(interfile)
		for _, kv := range bucket {
			err = enc.Encode(&kv)
			if err != nil {
				log.Printf("cannot encode key/value pair '%v'", kv)
				return false
			}
		}
		_ = interfile.Close()
		interfilename := fmt.Sprintf("mr-%v-%v", taskid, reduceID)
		err = os.Rename(tmpname, interfilename)
		if err != nil {
			log.Printf("cannot create intermediate file")
			return false
		}
	}

	return true
}

// for sorting by key.
type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleReduce(
	reducef func(string, []string) string,
	taskid int, nmap int) bool {

	intermediate := []KeyValue{}

	for m := 0; m < nmap; m++ {
		interfname := fmt.Sprintf("mr-%d-%d", m, taskid)
		interf, err := os.Open(interfname)
		if err != nil {
			log.Printf("cannot open '%v'", interfname)
			return false
		}
		dec := json.NewDecoder(interf)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Printf("cannot decode key/value pair '%v'", kv)
				return false
			}
			intermediate = append(intermediate, kv)
		}
		interf.Close()
	}

	sort.Sort(byKey(intermediate))

	ofile, err := ioutil.TempFile(".", "temp-*")
	if err != nil {
		log.Printf("cannot create an output file")
		return false
	}
	tmpname := ofile.Name()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		if _, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output); err != nil {
			log.Printf("cannot write output")
			return false
		}

		i = j
	}

	ofile.Close()

	ofilename := fmt.Sprintf("mr-out-%d", taskid)
	err = os.Rename(tmpname, ofilename)
	if err != nil {
		log.Printf("cannot create an output file")
		return false
	}

	return true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
