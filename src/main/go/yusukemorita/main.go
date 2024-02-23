package main

import (
	"bufio"
	"fmt"
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
	linesChannel := make(chan []string, 100)
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

func readFile(linesChannel chan []string) {
	file, err := os.Open("../../../../data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// limit := 10_000_000
	// counter := 0

	// batch together `batchSize` lines of text to send to the channel
	var batch []string

	for scanner.Scan() {
		batch = append(batch, scanner.Text())
		if len(batch) >= batchSize {
			linesChannel <- batch
			batch = []string{}
		}

		// counter++
		// if counter >= limit {
		// 	break
		// }
	}
	close(linesChannel)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func processLine(textChannel chan []string) (cityNames Set, cityCollection CityCollection) {
	cityCollection = NewCityCollection()
	cityNames = NewSet()

	for lines := range textChannel {
		for _, line := range lines {
			values := strings.Split(line, ";")
			if len(values) != 2 {
				log.Fatalf("unexpected values: %s", line)
			}

			cityName := values[0]
			temperature := parseTemperature(values[1])

			cityNames.Add(cityName)
			cityCollection.Add(cityName, temperature)
		}
	}

	return cityNames, cityCollection
}

// "41.1" -> 411
func parseTemperature(s string) int {
	withoutDot := strings.Replace(s, ".", "", 1)

	integer, err := strconv.ParseInt(withoutDot, 10, 0)
	if err != nil {
		log.Fatal(err)
	}

	return int(integer)
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
