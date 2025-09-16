package main

import (
	"fmt"
	dcpcouchbase "github.com/Trendyol/go-dcp-couchbase"
	"github.com/Trendyol/go-dcp-couchbase/couchbase"
	"github.com/bytedance/sonic"
)

func CustomMapperExample(ctx couchbase.EventContext) []couchbase.CBActionDocument {

	/*

		When using the mutateIn function to update a Couchbase document, any primitive Go value
		(like an int, string, or bool) must be converted into a JSON-formatted byte array.

		Couchbase stores data as JSON. To ensure data integrity, the values you send must be in a format Couchbase
		can correctly parse and integrate into the existing JSON structure.

		Strings: JSON strings must be wrapped in double quotes (").
		Marshaling a Go string like "hello" will produce a byte array for "\"hello\"", including the quotes.
		Sending a raw byte array without these quotes will likely break the JSON document.

		Numbers and Booleans: For numbers (int, float) and booleans (true, false), JSON format does not require quotes.
		While a raw byte array for 3 or true might coincidentally be the same as its JSON-marshalled version, relying on this is risky and inconsistent.

		For a safe and consistent approach, you should always JSON marshal primitive types before using them in a mutateIn operation.
		This guarantees that Couchbase receives the value in the correct format, preventing data corruption and ensuring future operations run smoothly.

	*/

	var intVal int = 3
	var boolVal bool = true
	var strVal string = "hello world"

	// JSON marshal primitive values
	intJSON, _ := sonic.Marshal(intVal)
	boolJSON, _ := sonic.Marshal(boolVal)
	strJSON, _ := sonic.Marshal(strVal)

	fmt.Println("=== JSON Marshal ===")
	fmt.Printf("int: %v\n", intJSON)    // [51] -> 3
	fmt.Printf("bool: %v\n", boolJSON)  // [116 114 117 101] -> true
	fmt.Printf("string: %v\n", strJSON) // [34 104 101 108 108 111 32 119 111 114 108 100 34] -> "hello world"

	// CBActionDocument examples
	actions := []couchbase.CBActionDocument{
		couchbase.NewMutateInAction([]byte("key1"), []byte("intPath"), intJSON),
		couchbase.NewMutateInAction([]byte("key2"), []byte("boolPath"), boolJSON),
		couchbase.NewMutateInAction([]byte("key3"), []byte("strPath"), strJSON),
	}

	return actions
}

func main() {
	connector, err := dcpcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(CustomMapperExample).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
