package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RandText(words int, wordLength int) string {
	s := ""
	for i := 0; i < words; i++ {
		if len(s) > 0 {
			s += " "
		}
		s += RandStringRunes(wordLength)
	}
	return s
}

func main() {
	nFiles := 8
	var wg sync.WaitGroup
	wg.Add(nFiles)
	for i := 0; i < nFiles; i++ {
		go func(i int) {
			defer wg.Done()

			f, err := os.Create(fmt.Sprintf("./input/part-%d.txt", i))
			if err != nil {
				panic(err)
			}
			_, err = f.Write([]byte(RandText(1000, 1)))
			if err != nil {
				panic(err)
			}
			err = f.Close()
			if err != nil {
				panic(err)
			}
		}(i)
	}
	wg.Wait()
}
