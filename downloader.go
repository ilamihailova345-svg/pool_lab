package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	ID  int
	URL string
}

type Result struct {
	JobID  int
	Status string
	WorkerID int
}

// имитирует скачивание, спит некоторое время
func download(url string) string {
	time.Sleep(500 * time.Millisecond)
	return "Скачано: " + url
}

// исполнитель
func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("Воркер %d начал смену\n", id)

	for job := range jobs {
		status := download(job.URL)
		results <- Result{JobID: job.ID, Status: status, WorkerID: id}
	}

	fmt.Printf("Воркер %d закончил смену\n", id)
}

func main() {
	urls := []string{
		"google.com",
		"yandex.ru",
		"github.com",
	}

	numWorkers := 3
	jobs := make(chan Job)
	results := make(chan Result, len(urls))

	var wg sync.WaitGroup

	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// Отправляем задачи в отдельной горутине.
	go func() {
		for i, url := range urls {
			jobs <- Job{ID: i + 1, URL: url}
		}
		close(jobs)
	}()

	// закрываем results после завершения
	go func() {
		wg.Wait()
		close(results)
	}()

	// читаем результаты
	for res := range results {
		fmt.Printf("Задача %d: %s (Скачивал воркер %d)\n",
			res.JobID, res.Status, res.WorkerID)
	}

	fmt.Println("Все задачи выполнены. Работа завершена.")
}