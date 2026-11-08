package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID     int
	Number int
}

type Result struct {
	JobID    int 
	Value    int 
	WorkerID int 
}

// считает факториал n!
func factorial(n int) int {
	result := 1
	for i := 2; i <= n; i++ { // начинаем с 2
		result *= i
	}
	return result
}

// jobs <-chan Job — для чтения
// results chan<- Result — для записи

// читает задачи из jobs, пишет результаты
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done() // уменьшает счётчик

	fmt.Printf("Воркер %d начал смену\n", id)

	// читает задачи, пока канал jobs не будет закрыт
	for job := range jobs {
		res := factorial(job.Number)
		// отправляем результат в канал results
		results <- Result{JobID: job.ID, Value: res, WorkerID: id}
	}

	fmt.Printf("Воркер %d закончил смену\n", id)
}

func main() {
	const (
		numWorkers = 3 
		numJobs    = 20
	)

	// канал задач небуферизированный
	jobs := make(chan Job)

	// канал результатов буферизированный на 5
	results := make(chan Result, 5)

	var wg sync.WaitGroup

	fmt.Printf("Запускаем %d воркеров\n", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// гортина-генератор отправляет задачи в jobs
	// параллельно, чтобы main мог сразу читать результаты
	go func() {
		fmt.Println("Начинаем отправку задач")
		for j := 1; j <= numJobs; j++ {
			jobs <- Job{ID: j, Number: j}
			time.Sleep(100 * time.Millisecond)
		}
		close(jobs)
		fmt.Println("Все задачи отправлены")
	}()

	// отдельная горутина ждёт завершения всех воркеров и закрывает results
	go func() {
		wg.Wait()
		close(results)
	}()

	// читает результаты, пока канал results не будет закрыт
	for res := range results {
		fmt.Printf("Задача %d: %d! = %d (Считал воркер %d)\n",
			res.JobID, res.JobID, res.Value, res.WorkerID)
	}

	fmt.Println("Все задачи выполнены. Работа завершена.")
}