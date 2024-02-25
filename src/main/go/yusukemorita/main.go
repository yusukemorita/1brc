package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"time"
)

type City struct {
	min   int
	max   int
	sum   int
	count int
}

type CityCollection struct {
	cities map[string]*City
}

func (collection CityCollection) Merge(cc CityCollection) CityCollection {
	new := cc.cities

	for cityName, cityB := range collection.cities {
		cityA, ok := new[cityName]
		if ok {
			if cityA.max < cityB.max {
				cityA.max = cityB.max
			}
			if cityA.min > cityB.min {
				cityA.min = cityB.min
			}
			cityA.count += cityB.count
			cityA.sum += cityB.sum
		} else {
			new[cityName] = cityB
		}
	}

	return CityCollection{cities: new}
}

func (collection CityCollection) Add(name string, temperature int) {
	city, ok := collection.cities[name]
	if ok {
		if city.min > temperature {
			city.min = temperature
		}
		if city.max < temperature {
			city.max = temperature
		}
		city.sum += temperature
		city.count++
	} else {
		collection.cities[name] = &City{
			min:   temperature,
			max:   temperature,
			sum:   temperature,
			count: 1,
		}
	}
}

func NewCityCollection() CityCollection {
	return CityCollection{
		cities: make(map[string]*City),
	}
}

type Result struct {
	cities CityCollection
}

const cpuProfile = "cpu13.prof"
const memoryProfile = "memory13.prof"
const concurrency = 4
const batchSize = 100

const chunkSize = 1 * 1024 * 1024 // 1mb
// const chunkSize = 500 * 1024 // 500kb

func main() {
	f, err := os.Create(cpuProfile)
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	startTime := time.Now()

	// Open the file
	linesChannel := make(chan string, 100)
	resultChannel := make(chan Result, concurrency)
	go readFile(linesChannel)

	waitGroup := new(sync.WaitGroup)
	waitGroup.Add(concurrency)

	// close result channel after receiving all results
	go func() {
		defer close(resultChannel)
		waitGroup.Wait()
	}()

	for i := 1; i <= concurrency; i++ {
		go func() {
			defer waitGroup.Done()
			cities := processLine(linesChannel)
			resultChannel <- Result{cities: cities}
		}()
	}

	allCities := NewCityCollection()
	for result := range resultChannel {
		allCities = allCities.Merge(result.cities)
	}

	var cityNames []string
	for cityName, _ := range allCities.cities {
		cityNames = append(cityNames, cityName)
	}
	slices.Sort(cityNames)

	for _, cityName := range cityNames {
		city := allCities.cities[cityName]
		mean := math.Ceil(float64(city.sum) / float64(city.count))
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", cityName, float64(city.min)/10, float64(mean/10), float64(city.max)/10)
	}

	fmt.Printf("\ntotal duration: %f seconds\n", time.Now().Sub(startTime).Seconds())

	f, err = os.Create(memoryProfile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	runtime.GC()    // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}

func readFile(linesChannel chan string) {
	file, err := os.Open("../../../../data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// limit := 500 * 1024 * 1024
	buffer := make([]byte, chunkSize)
	finished := false
	var readCount int64 = 0

	for {
		if finished {
			break
		}

		count, err := file.ReadAt(buffer, readCount)
		if err == io.EOF {
			finished = true
		} else if err != nil {
			log.Fatal(err)
		}
		readCount += int64(count)

		// read up to the next new line
		var extra []byte
		for {
			singleCharacterBuffer := make([]byte, 1)
			_, err = file.ReadAt(singleCharacterBuffer, readCount)
			if err == io.EOF {
				finished = true
				break
			} else if err != nil {
				log.Fatal(err)
			}
			readCount++
			extra = append(extra, singleCharacterBuffer...)
			if singleCharacterBuffer[0] == '\n' {
				break
			}
		}

		// remove null bytes from string, which can happen when
		// the end of the file has been reached and the buffer is not full.
		buffer = bytes.Trim(buffer, "\x00")
		linesChannel <- string(buffer) + string(extra)
		buffer = make([]byte, chunkSize)

		// if int(readCount) >= limit {
		// 	break
		// }
	}
	close(linesChannel)
}

func processLine(textChannel chan string) (cityCollection CityCollection) {
	cityCollection = NewCityCollection()

	for linesString := range textChannel {
		for {
			line, remaining, found := strings.Cut(linesString, "\n")
			if !found {
				// end of string reached
				break
			}
			linesString = remaining

			cityName, temperaturesString, found := strings.Cut(line, ";")
			if !found {
				log.Fatalf("unexpected values: %s", line)
			}
			cityCollection.Add(cityName, parseTemperature(temperaturesString))
		}
	}

	return cityCollection
}

// "41.1" -> 411
// assume 3 digits if positive, and 4 digits if negative
func parseTemperature(s string) int {
	integerString, decimalString, found := strings.Cut(s, ".")
	if !found {
		log.Fatalf("dot not found: %s", s)
	}

	positive := integerString[0] != '-'
	if !positive {
		integerString = integerString[1:]
	}

	var integer int
	if len(integerString) == 2 {
		integer = convertTwoDigits(integerString)
	} else {
		integer = convertOneDigit(integerString[0])
	}

	decimal := convertOneDigit(decimalString[0])

	absolute := integer*10 + decimal

	if positive {
		return absolute
	} else {
		return -1 * absolute
	}
}

func convertTwoDigits(s string) int {
	highDigit := convertOneDigit(s[0])
	lowDigit := convertOneDigit(s[1])

	return highDigit*10 + lowDigit
}

func convertOneDigit(b byte) int {
	b -= '0'
	return int(b)
}
