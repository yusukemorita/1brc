package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type City struct {
	min   int
	max   int
	sum   int
	count int
}

func main() {
	startTime := time.Now()

	// Open the file
	textChannel := make(chan string, 100)

	go readFile(textChannel)

	result := make(map[string]*City)

	cityNames := NewSet()

	// Loop over all lines in the file
	loopStart := time.Now()
	for line := range textChannel {
		if strings.HasPrefix(line, "#") {
			// ignore comments
			continue
		}

		values := strings.Split(line, ";")
		if len(values) != 2 {
			log.Fatalf("unexpected values: %s", line)
		}

		cityName := values[0]
		temperature := parseTemperature(values[1])

		cityNames.Add(cityName)

		city, ok := result[cityName]
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
			result[cityName] = &City{
				min:   temperature,
				max:   temperature,
				sum:   temperature,
				count: 1,
			}
		}
	}

	loopEnd := time.Now()

	citySortStart := time.Now()
	allCityNames := cityNames.ToSlice()
	slices.Sort(allCityNames)
	citySortEnd := time.Now()

	calculateStart := time.Now()
	for _, cityName := range allCityNames {
		city := result[cityName]
		mean := math.Ceil(float64(city.sum) / float64(city.count))
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", cityName, float64(city.min)/10, float64(mean/10), float64(city.max)/10)
	}
	calculateEnd := time.Now()

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("total duration: %f seconds\n", duration.Seconds())
	fmt.Printf("loop duration: %f seconds\n", loopEnd.Sub(loopStart).Seconds())
	fmt.Printf("city sort duration: %f seconds\n", citySortEnd.Sub(citySortStart).Seconds())
	fmt.Printf("calculate duration: %f seconds\n", calculateEnd.Sub(calculateStart).Seconds())
}

func readFile(textChannel chan string) {
	file, err := os.Open("../../../../data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// limit := 10_000_000
	// counter := 0
	for scanner.Scan() {
		textChannel <- scanner.Text()
		// counter++
		// if counter >= limit {
		// 	break
		// }
	}
	close(textChannel)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
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
