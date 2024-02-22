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

const limit = 10_000_000_000

func main() {
	startTime := time.Now()

	// Open the file
	file, err := os.Open("../../../../data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close() // Make sure to close the file when you're done

	// Create a new Scanner for the file
	scanner := bufio.NewScanner(file)

	counter := 0

	result := make(map[string][]int)

	cityNames := NewSet()

	// Loop over all lines in the file
	loopStart := time.Now()
	for scanner.Scan() {
		counter++
		line := scanner.Text()

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

		result[cityName] = append(result[cityName], temperature)
		cityNames.Add(cityName)

		// fmt.Printf("appending %s, %d\n", city, temperature)

		if counter >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	loopEnd := time.Now()

	citySortStart := time.Now()
	allCityNames := cityNames.ToSlice()
	slices.Sort(allCityNames)
	citySortEnd := time.Now()

	calculateStart := time.Now()
	for _, cityName := range allCityNames {
		temperatures := result[cityName]
		mean := math.Ceil(float64(sum(temperatures)) / float64(len(temperatures)))
		max, min := maxAndMin(temperatures)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", cityName, float64(min)/10, float64(mean)/10, float64(max)/10)
	}
	calculateEnd := time.Now()

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("total duration: %f seconds\n", duration.Seconds())
	fmt.Printf("loop duration: %f seconds\n", loopEnd.Sub(loopStart).Seconds())
	fmt.Printf("city sort duration: %f seconds\n", citySortEnd.Sub(citySortStart).Seconds())
	fmt.Printf("calculate duration: %f seconds\n", calculateEnd.Sub(calculateStart).Seconds())
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

func sum(numbers []int) int {
	s := 0

	for _, number := range numbers {
		s += number
	}

	return s
}

func maxAndMin(numbers []int) (max, min int) {
	currentMax := numbers[0]
	currentMin := numbers[0]
	for _, n := range numbers {
		if n > currentMax {
			currentMax = n
		}
		if n < currentMin {
			currentMin = n
		}
	}
	return currentMax, currentMin
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
