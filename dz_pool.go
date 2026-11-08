package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	ID     int 
	Number int 
}

type Result struct {
	JobID    int
	Input    int
	S     int 
	WorkerID int
}

// считает площадь и отправляет результат
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		sleepTime := time.Duration(rand.Intn(5)+1) * 100 * time.Millisecond
		time.Sleep(sleepTime)

		S := job.Number * job.Number

		results <- Result{
			JobID:    job.ID,
			Input:    job.Number,
			S:     S,
			WorkerID: id,
		}
	}
}

func main() {
	inputs := []int{1, 5, 12, 5, 3, 8, 9}

	jobs := make(chan Job)
	results := make(chan Result)

	var wg sync.WaitGroup

	const numWorkers = 3
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// генератор задач 
	go func() {
		for i, num := range inputs {
			jobs <- Job{ID: i + 1, Number: num}
		}
		close(jobs)
	}()

	// наблюдатель ждёт завершения всех воркеров и закрывает results
	go func() {
		wg.Wait()
		close(results)
	}()

	fmt.Println("--- Результаты обработки ---")
	for res := range results {
		fmt.Printf("Задача %d: квадрат %d = %d (воркер %d)\n",
			res.JobID, res.Input, res.S, res.WorkerID)
	}

	fmt.Println("Все задачи выполнены.")
}