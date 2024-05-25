package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	numGoroutines     = 4
	numEjecuciones    = 1000
	mostrarResultados = 10
)

type Point struct {
	AssessedValue float64
	SaleAmount    float64
}

func leerDatosDesdeCSV(url string) ([]Point, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	reader := csv.NewReader(resp.Body)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	headers := records[0]
	var assessedValueIndex, saleAmountIndex int
	for i, header := range headers {
		switch header {
		case "Assessed Value":
			assessedValueIndex = i
		case "Sale Amount":
			saleAmountIndex = i
		}
	}

	var points []Point
	for _, record := range records[1:] {
		assessedValue, err := strconv.ParseFloat(record[assessedValueIndex], 64)
		if err != nil {
			return nil, err
		}
		saleAmount, err := strconv.ParseFloat(record[saleAmountIndex], 64)
		if err != nil {
			return nil, err
		}
		points = append(points, Point{AssessedValue: assessedValue, SaleAmount: saleAmount})
	}
	return points, nil
}

func linearRegression(points []Point) (m float64, b float64) {
	var sumX, sumY, sumXY, sumX2, n float64
	n = float64(len(points))
	for _, p := range points {
		sumX += p.AssessedValue
		sumY += p.SaleAmount
		sumXY += p.AssessedValue * p.SaleAmount
		sumX2 += p.AssessedValue * p.AssessedValue
	}
	if denom := (n*sumX2 - sumX*sumX); denom != 0 {
		m = (n*sumXY - sumX*sumY) / denom
		b = (sumY - m*sumX) / n
	} else {
		fmt.Println("Revisar datos de entrada")
	}
	return
}

func linearRegressionConcurrente(points []Point) (m float64, b float64) {
	var sumX, sumY, sumXY, sumX2 float64
	n := float64(len(points))
	segmentSize := len(points) / numGoroutines

	sumChan := make(chan float64, numGoroutines*4)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		start := i * segmentSize
		end := start + segmentSize
		if i == numGoroutines-1 {
			end = len(points)
		}

		wg.Add(1)
		go func(pts []Point) {
			defer wg.Done()
			var sumXLocal, sumYLocal, sumXYLocal, sumX2Local float64
			for _, p := range pts {
				sumXLocal += p.AssessedValue
				sumYLocal += p.SaleAmount
				sumXYLocal += p.AssessedValue * p.SaleAmount
				sumX2Local += p.AssessedValue * p.AssessedValue
			}
			sumChan <- sumXLocal
			sumChan <- sumYLocal
			sumChan <- sumXYLocal
			sumChan <- sumX2Local
		}(points[start:end])
	}

	wg.Wait()
	close(sumChan)

	for val := range sumChan {
		sumX += val
		sumY += <-sumChan
		sumXY += <-sumChan
		sumX2 += <-sumChan
	}

	if denom := (n*sumX2 - sumX*sumX); denom != 0 {
		m = (n*sumXY - sumX*sumY) / denom
		b = (sumY - m*sumX) / n
	} else {
		fmt.Println("Denominador cero detectado, revisar datos de entrada")
	}
	return
}

func main() {
	// Todo los datos
	testDataset("https://raw.githubusercontent.com/educaba123/ta3-archivos/main/real_estate.csv", "todos los datos", -1)

	// los primeros 1000 datos
	testDataset("https://raw.githubusercontent.com/educaba123/ta3-archivos/main/real_estate.csv", "1000 datos", 1000)
	//datos sin outliers
	testDataset("https://raw.githubusercontent.com/educaba123/ta3-archivos/main/df_no_outliers2.csv", "todos los datos sin outliers", -1)
	testDataset("https://raw.githubusercontent.com/educaba123/ta3-archivos/main/df_no_outliers2.csv", "1000 datos sin outliers", 1000)
}

func testDataset(url, description string, limit int) {
	var totalMSec, totalBSec, totalMCon, totalBCon float64
	var totalTiempoSec, totalTiempoCon time.Duration

	puntos, err := leerDatosDesdeCSV(url)
	if err != nil {
		fmt.Println("Error al leer el archivo CSV:", err)
		return
	}

	if limit > 0 && len(puntos) > limit {
		puntos = puntos[:limit]
	}

	for i := 0; i < numEjecuciones; i++ {
		if i == 0 {
			fmt.Printf("Primeros 100 puntos de la primera ejecución (%s):\n", description)
			for j := 0; j < 100 && j < len(puntos); j++ {
				fmt.Printf("Punto %d: Assessed Value = %.2f, Sale Amount = %.2f\n", j+1, puntos[j].AssessedValue, puntos[j].SaleAmount)
			}
		}

		start := time.Now()
		mSec, bSec := linearRegression(puntos)
		durationSec := time.Since(start)
		totalTiempoSec += durationSec

		start = time.Now()
		mCon, bCon := linearRegressionConcurrente(puntos)
		durationCon := time.Since(start)
		totalTiempoCon += durationCon

		totalMSec += mSec
		totalBSec += bSec
		totalMCon += mCon
		totalBCon += bCon

		if i < mostrarResultados {
			fmt.Printf("Ejecución %d (%s) - Secuencial: Pendiente = %.2f, Intercepto = %.2f, Tiempo = %v\n", i+1, description, mSec, bSec, durationSec)
			fmt.Printf("Ejecución %d (%s) - Concurrente: Pendiente = %.2f, Intercepto = %.2f, Tiempo = %v\n", i+1, description, mCon, bCon, durationCon)
		}
	}

	promedioMSec := totalMSec / float64(numEjecuciones)
	promedioBSec := totalBSec / float64(numEjecuciones)
	promedioMCon := totalMCon / float64(numEjecuciones)
	promedioBCon := totalBCon / float64(numEjecuciones)

	fmt.Printf("Promedio Secuencial (%s) - Pendiente: %.2f, Intercepto: %.2f, Tiempo total: %v\n", description, promedioMSec, promedioBSec, totalTiempoSec)
	fmt.Printf("Promedio Concurrente (%s) - Pendiente: %.2f, Intercepto: %.2f, Tiempo total: %v\n", description, promedioMCon, promedioBCon, totalTiempoCon)
}
