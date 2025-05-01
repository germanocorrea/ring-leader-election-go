// Código exemplo para o trabaho de sistemas distribuidos (eleicao em anel)
// By Cesar De Rose - 2022

package main

import (
	"fmt"
	"sync"
)

type mensagem struct {
	tipo  int // tipo da mensagem para fazer o controle do que fazer (eleição, confirmacao da eleicao)
	corpo int // conteudo da mensagem para colocar os ids (usar um tamanho compativel com o numero de processos no anel)
}

const (
	TIPO_MSG_FORCE_FALHA        int = 2
	TIPO_MSG_FORCE_RETURN       int = 3
	TIPO_MSG_INFORM_LEADER_DOWN int = 4
	TIPO_MSG_VOTE_ELECTION      int = 5
	TIPO_MSG_CONFIRM_ELECTION   int = 6
)

var (
	chans = []chan mensagem{ // vetor de canias para formar o anel de eleicao - chan[0], chan[1] and chan[2] ...
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
	}
	controle = make(chan int)
	wg       sync.WaitGroup // wg is used to wait for the program to finish
)

func ElectionControler(in chan int) {
	defer wg.Done()

	var temp mensagem

	// comandos para o anel iciam aqui

	// mudar o processo 0 - canal de entrada 3 - para falho (defini mensagem tipo 2 pra isto)

	temp.tipo = 2
	chans[3] <- temp
	fmt.Printf("Controle: mudar o processo 0 para falho\n")

	fmt.Printf("Controle: confirmação %d\n", <-in) // receber e imprimir confirmação

	// mudar o processo 1 - canal de entrada 0 - para falho (defini mensagem tipo 2 pra isto)

	temp.tipo = 2
	chans[0] <- temp
	fmt.Printf("Controle: mudar o processo 1 para falho\n")
	fmt.Printf("Controle: confirmação %d\n", <-in) // receber e imprimir confirmação

	// matar os outrs processos com mensagens não conhecidas (só pra cosumir a leitura)

	temp.tipo = -1
	chans[1] <- temp
	chans[2] <- temp

	fmt.Println("\n   Processo controlador concluído\n")
}

func ElectionStage(TaskId int, in chan mensagem, out chan mensagem, leader int) {
	defer wg.Done()

	var actualLeader int
	var bFailed bool = false
	var selfDispatchedElection bool = false

	actualLeader = leader

	var keepListening bool = true
	for keepListening {
		inboundMessage := <-in

		if bFailed {
			out <- inboundMessage
			continue
		}

		fmt.Printf("%2d: recebi mensagem %d, [ %d ]\n", TaskId, inboundMessage.tipo, inboundMessage.corpo)

		switch inboundMessage.tipo {
		case TIPO_MSG_FORCE_FALHA:
			{
				bFailed = true
				fmt.Printf("%2d: falho %v \n", TaskId, bFailed)
				fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
				controle <- -5
			}
		case TIPO_MSG_FORCE_RETURN:
			{
				bFailed = false
				fmt.Printf("%2d: falho %v \n", TaskId, bFailed)
				fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
				controle <- -5
			}
		case TIPO_MSG_INFORM_LEADER_DOWN:
			{
				selfDispatchedElection = true
				out <- mensagem{
					tipo:  TIPO_MSG_VOTE_ELECTION,
					corpo: TaskId,
				}
			}
		case TIPO_MSG_VOTE_ELECTION:
			{
				if selfDispatchedElection {
					selfDispatchedElection = false
					out <- mensagem{
						tipo:  TIPO_MSG_CONFIRM_ELECTION,
						corpo: inboundMessage.corpo,
					}
				} else {
					if inboundMessage.corpo < TaskId {
						inboundMessage.corpo = TaskId
					}
					out <- inboundMessage
				}
			}
		default:
			{
				fmt.Printf("%2d: não conheço este tipo de mensagem\n", TaskId)
				fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
				keepListening = false
			}
		}
	}

	fmt.Printf("%2d: terminei \n", TaskId)
}

func main() {

	wg.Add(5) // Add a count of four, one for each goroutine

	// criar os processo do anel de eleicao

	go ElectionStage(0, chans[3], chans[0], 0) // este é o lider
	go ElectionStage(1, chans[0], chans[1], 0) // não é lider, é o processo 0
	go ElectionStage(2, chans[1], chans[2], 0) // não é lider, é o processo 0
	go ElectionStage(3, chans[2], chans[3], 0) // não é lider, é o processo 0

	fmt.Println("\n   Anel de processos criado")

	// criar o processo controlador

	go ElectionControler(controle)

	fmt.Println("\n   Processo controlador criado\n")

	wg.Wait() // Wait for the goroutines to finish\
}
