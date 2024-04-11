package main
import "fmt"

func main() {
	m := make(map[string]string)
	m["123"] = "4"

	fmt.Printf("%v\n", m["5"])
	fmt.Printf("%v\n", m["123"])
}