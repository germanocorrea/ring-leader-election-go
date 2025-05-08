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
	TIPO_MSG_FORCE_FALHA      int = 2
	TIPO_MSG_FORCE_RETURN     int = 3
	TIPO_MSG_VOTE_ELECTION    int = 4
	TIPO_MSG_CONFIRM_ELECTION int = 5
	TIPO_MSG_FINISH_PROCESS   int = 6
)

var (
	chans = []chan mensagem{ // vetor de canias para formar o anel de eleicao - chan[0], chan[1] and chan[2] ...
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
	}
	controle       = make(chan int)
	firstLeader    = 3
	highestProcess = 3
	wg             sync.WaitGroup // wg is used to wait for the program to finish
)

func normalizeProcessId(TaskId int) int {
	if TaskId > highestProcess {
		return 0
	} else if TaskId < 0 {
		return highestProcess
	}

	return TaskId
}

func getInputChannelForTaskId(TaskId int) int {
	return normalizeProcessId(TaskId - 1)
}

func derrubarProcesso(controle chan int, TaskId int) {
	fmt.Printf("Controle: mudar o processo %d para falho\n", TaskId)
	inputChannel := getInputChannelForTaskId(TaskId)
	chans[inputChannel] <- mensagem{
		tipo: TIPO_MSG_FORCE_FALHA,
	}
	fmt.Printf("Controle: confirmação do processo %d\n", <-controle)
}

func voltaProcesso(controle chan int, TaskId int) {
	fmt.Printf("Controle: retorna processo %d\n", TaskId)
	inputChannel := getInputChannelForTaskId(TaskId)
	chans[inputChannel] <- mensagem{
		tipo: TIPO_MSG_FORCE_RETURN,
	}
	fmt.Printf("Controle: confirmação do processo %d\n", <-controle)
}

func disparaEleicao(controle chan int, TaskId int) {
	fmt.Printf("Controle: dispara eleição no processo %d\n", TaskId)
	inputChannel := getInputChannelForTaskId(TaskId)
	chans[inputChannel] <- mensagem{
		tipo:  TIPO_MSG_VOTE_ELECTION,
		corpo: -1,
	}
	fmt.Printf("Controle: confirmação da eleição vinda do processo %d\n", <-controle)
}

func finalizaProcessos() {
	fmt.Printf("Disparando finalizações")

	chans[0] <- mensagem{tipo: TIPO_MSG_FINISH_PROCESS}
	chans[1] <- mensagem{tipo: TIPO_MSG_FINISH_PROCESS}
	chans[2] <- mensagem{tipo: TIPO_MSG_FINISH_PROCESS}
	chans[3] <- mensagem{tipo: TIPO_MSG_FINISH_PROCESS}
}

func ElectionControler(in chan int) {
	defer wg.Done()

	fmt.Println("\n\n\n============ Teste 1: derrubar e retornar líder, eleição deve escolher novo enquanto está fora e voltar para o líder original depois\n")
	derrubarProcesso(in, firstLeader)
	disparaEleicao(in, normalizeProcessId(firstLeader+1))
	voltaProcesso(in, firstLeader)
	disparaEleicao(in, firstLeader)

	fmt.Println("\n\n\n============ Teste 2: derruba processo menor, eleição deve manter mesmo líder\n")
	derrubarProcesso(in, 0)
	disparaEleicao(in, 1)
	voltaProcesso(in, 0)
	disparaEleicao(in, 1)

	fmt.Println("\n\n\n============ Teste 3: derruba todos processos menos um\n")
	derrubarProcesso(in, 0)
	derrubarProcesso(in, 1)
	derrubarProcesso(in, 3)
	disparaEleicao(in, 2)
	voltaProcesso(in, 0)
	voltaProcesso(in, 1)
	voltaProcesso(in, 3)
	disparaEleicao(in, 0)

	finalizaProcessos()

	fmt.Println("\n   Processo controlador concluído\n")
}

func ElectionStage(TaskId int, in chan mensagem, out chan mensagem, leader int, colorCode string) {
	defer wg.Done()

	var actualLeader int
	var bFailed = false
	var alreadyVoted = false
	var alreadyConfirmed = false

	actualLeader = leader

	var keepListening = true
	for keepListening {
		fmt.Printf(colorCode+"%2d: aguardando mensagens\n\033[0m", TaskId)
		inboundMessage := <-in
		fmt.Printf(colorCode+"%2d: mensagem recebida!\n\033[0m", TaskId)

		fmt.Printf(colorCode+"%2d: recebi mensagem do tipo %d, conteúdo: [ %d ]\n\033[0m", TaskId, inboundMessage.tipo, inboundMessage.corpo)

		if bFailed && inboundMessage.tipo != TIPO_MSG_FORCE_RETURN {
			fmt.Printf(colorCode+"%2d: estou fora, pulando para o próximo\n\033[0m", TaskId)
			out <- inboundMessage
			continue
		}

		switch inboundMessage.tipo {
		case TIPO_MSG_FORCE_FALHA:
			{
				bFailed = true
				fmt.Printf(colorCode+"%2d: falhei \n\033[0m", TaskId)
				fmt.Printf(colorCode+"%2d: lider atual %d\n\033[0m", TaskId, actualLeader)
				controle <- TaskId
			}
		case TIPO_MSG_FORCE_RETURN:
			{
				bFailed = false
				fmt.Printf(colorCode+"%2d: voltei da falha \n\033[0m", TaskId)
				fmt.Printf(colorCode+"%2d: lider atual %d\n\033[0m", TaskId, actualLeader)
				controle <- TaskId
			}
		case TIPO_MSG_VOTE_ELECTION:
			{
				alreadyConfirmed = false
				if alreadyVoted {
					alreadyVoted = false
					out <- mensagem{
						tipo:  TIPO_MSG_CONFIRM_ELECTION,
						corpo: inboundMessage.corpo,
					}
					fmt.Printf(colorCode+"%2d: eu disparei a eleição, enviando confirmação do vencedor: %2d\n\033[0m", TaskId, inboundMessage.corpo)
				} else {
					alreadyVoted = true
					if inboundMessage.corpo < TaskId {
						fmt.Printf(colorCode+"%2d: tenho prioridade em relação a %2d, me colocando na eleição\n\033[0m", TaskId, inboundMessage.corpo)
						inboundMessage.corpo = TaskId
					} else {
						fmt.Printf(colorCode+"%2d: TaskId na eleição com mais prioridade que eu\n\033[0m", TaskId)
					}
					out <- inboundMessage
				}
			}
		case TIPO_MSG_CONFIRM_ELECTION:
			{
				alreadyVoted = false
				if !alreadyConfirmed {
					alreadyConfirmed = true
					fmt.Printf(colorCode+"%2d: confirmando resultado da eleição, processo líder é %2d\n\033[0m", TaskId, inboundMessage.corpo)
					actualLeader = inboundMessage.corpo
					out <- inboundMessage
				} else {
					controle <- TaskId
				}
			}
		case TIPO_MSG_FINISH_PROCESS:
			{
				fmt.Printf(colorCode+"%2d: finalizando o processo\n\033[0m", TaskId)
				keepListening = false
			}
		default:
			{
				fmt.Printf(colorCode+"%2d: não conheço este tipo de mensagem\n\033[0m", TaskId)
				fmt.Printf(colorCode+"%2d: lider atual %d\n\033[0m", TaskId, actualLeader)
				keepListening = false
			}
		}
	}

	fmt.Printf(colorCode+"%2d: terminei \n\033[0m", TaskId)
}

func main() {

	wg.Add(5) // Add a count of four, one for each goroutine

	// criar os processo do anel de eleicao

	go ElectionStage(0, chans[3], chans[0], 0, "\033[31m") // este é o lider
	go ElectionStage(1, chans[0], chans[1], 0, "\033[34m") // não é lider, é o processo 0
	go ElectionStage(2, chans[1], chans[2], 0, "\033[33m") // não é lider, é o processo 0
	go ElectionStage(3, chans[2], chans[3], 0, "\033[32m") // não é lider, é o processo 0

	fmt.Println("\n   Anel de processos criado")

	// criar o processo controlador

	go ElectionControler(controle)

	fmt.Println("\n   Processo controlador criado\n")

	wg.Wait() // Wait for the goroutines to finish\
}
