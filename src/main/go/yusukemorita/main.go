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
	"strconv"
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
	cityNames Set
	cities    CityCollection
}

const cpuProfile = "cpu.prof"
const memoryProfile = "memory.prof"
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

	var processStart, processEnd time.Time
	// close result channel after receiving all results
	go func() {
		processStart = time.Now()
		defer close(resultChannel)
		waitGroup.Wait()
		processEnd = time.Now()
	}()

	for i := 1; i <= concurrency; i++ {
		go func() {
			defer waitGroup.Done()
			names, cities := processLine(linesChannel)
			resultChannel <- Result{cityNames: names, cities: cities}
		}()
	}

	var mergeDuration time.Duration
	allCityNames := NewSet()
	allCities := NewCityCollection()
	for result := range resultChannel {
		start := time.Now()
		allCityNames.Merge(result.cityNames)
		allCities = allCities.Merge(result.cities)
		end := time.Now()
		mergeDuration += end.Sub(start)
	}

	citySortStart := time.Now()
	sortedCityNames := allCityNames.ToSlice()
	slices.Sort(sortedCityNames)
	citySortEnd := time.Now()

	calculateStart := time.Now()
	for _, cityName := range sortedCityNames {
		city := allCities.cities[cityName]
		mean := math.Ceil(float64(city.sum) / float64(city.count))
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", cityName, float64(city.min)/10, float64(mean/10), float64(city.max)/10)
	}
	calculateEnd := time.Now()

	endTime := time.Now()
	fmt.Printf("total duration: %f seconds\n", endTime.Sub(startTime).Seconds())
	fmt.Printf("process duration: %f seconds\n", processEnd.Sub(processStart).Seconds())
	fmt.Printf("merge duration: %f seconds\n", mergeDuration.Seconds())
	fmt.Printf("city sort duration: %f seconds\n", citySortEnd.Sub(citySortStart).Seconds())
	fmt.Printf("calculate duration: %f seconds\n", calculateEnd.Sub(calculateStart).Seconds())

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

func processLine(textChannel chan string) (cityNames Set, cityCollection CityCollection) {
	cityCollection = NewCityCollection()
	cityNames = NewSet()

	for linesString := range textChannel {
		for {
			newLineIndex := strings.Index(linesString, "\n")
			if newLineIndex == -1 {
				// end of string reached
				break
			}

			line := linesString[:newLineIndex]
			linesString = linesString[newLineIndex+1:]

			separatorIndex := strings.Index(line, ";")
			if separatorIndex == -1 {
				log.Fatalf("unexpected values: %s", line)
			}

			cityName := line[:separatorIndex]
			temperature := parseTemperature(line[separatorIndex+1:])

			cityNames.Add(cityName)
			cityCollection.Add(cityName, int(temperature))
		}
	}

	return cityNames, cityCollection
}

// "41.1" -> 411
// assume 3 digits if positive, and 4 digits if negative
func parseTemperature(s string) int64 {
	integerString, decimalString, found := strings.Cut(s, ".")
	if !found {
		log.Fatalf("dot not found: %s", s)
	}

	integer, err := strconv.ParseInt(integerString, 10, 0)
	if err != nil {
		log.Println(err)
		log.Fatalf("error parsing: %s", s)
	}

	decimal, err := strconv.ParseInt(decimalString, 10, 0)
	if err != nil {
		log.Println(err)
		log.Fatalf("error parsing: %s", s)
	}

	if integer >= 0 {
		return integer * 10 + decimal
	} else {
		return integer * 10 - decimal
	}
}

func NewSet() Set {
	return Set{
		values: make(map[string]bool),
	}
}

type Set struct {
	values map[string]bool
}

func (set Set) Add(s string) {
	set.values[s] = true
}

func (set Set) ToSlice() []string {
	var slice []string
	for k, _ := range set.values {
		slice = append(slice, k)
	}

	return slice
}

func (set Set) Merge(otherSet Set) {
	for key, _ := range otherSet.values {
		set.Add(key)
	}
}
