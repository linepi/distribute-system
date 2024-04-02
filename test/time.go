package main
import "time"
import "fmt"

func main() {
	t := time.Now()
	time.Sleep(time.Second)
	if time.Now().Sub(t) > time.Second {
		fmt.Println("yes")
	}
}