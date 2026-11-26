// broker é o executável do Maré Pub/Sub (passo 8).
//
// Uso:
//
//	broker -role principal -addr :7000 -data ./dados-principal
//	broker -role reserva   -addr :7001 -data ./dados-reserva
//	broker -promote localhost:7001      (operador: promove a reserva)
//
// Ctrl+C encerra com limpeza: para de aceitar conexões, fecha as abertas e
// fecha o log. O que foi confirmado continua no log.
//
// O log fica em disco, no wal do Felipe (adaptador em broker/walstore): um arquivo
// por época em DATA/log-<época>.wal. Reiniciar com a mesma pasta recupera
// tópicos, assinaturas e entregas pendentes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/server"
	"github.com/GabriellCarvallho/mare-pubsub/broker/walstore"
)

func main() {
	// Ctrl+C (SIGINT) ou kill (SIGTERM) cancelam o ctx: o Run encerra limpo.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}

// options são as opções da linha de comando.
type options struct {
	addr     string
	role     string
	dataDir  string
	recovery time.Duration
	promote  string
}

// parseFlags lê as opções. Separado do main para os testes poderem chamar.
func parseFlags(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.addr, "addr", ":7000", "endereço TCP onde o broker escuta")
	fs.StringVar(&o.role, "role", server.RolePrincipal, `papel: "principal" ou "reserva"`)
	fs.StringVar(&o.dataDir, "data", "./dados", "pasta da época e do log")
	fs.DurationVar(&o.recovery, "recovery", server.DefaultRecovery, "janela RECOVERING após a promoção")
	fs.StringVar(&o.promote, "promote", "", "promove a reserva neste endereço e sai (ex.: localhost:7001)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "uso:")
		fmt.Fprintln(stderr, "  broker [-role principal|reserva] [-addr :7000] [-data ./dados] [-recovery 30s]")
		fmt.Fprintln(stderr, "  broker -promote HOST:PORTA")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("argumento inesperado %q", fs.Arg(0))
	}
	return o, nil
}

// newConfig monta a configuração do servidor a partir das opções.
func newConfig(o options, stdout io.Writer) server.Config {
	recovery := o.recovery
	if recovery == 0 {
		recovery = -1 // -recovery 0 = sem janela (no server, 0 quer dizer "padrão")
	}
	return server.Config{
		Addr:     o.addr,
		Role:     o.role,
		DataDir:  o.dataDir,
		Recovery: recovery,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(stdout, time.Now().Format("15:04:05 ")+format+"\n", args...)
		},
		OpenLog: walstore.Open, // log em disco (wal do Felipe)
	}
}

// run é o programa inteiro, sem os.Exit: os testes chamam direto.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil // -h: a ajuda já foi impressa
	}
	if err != nil {
		return err
	}

	// Modo operador: promove a reserva e sai.
	if o.promote != "" {
		epoch, err := server.Promote(o.promote)
		if err != nil {
			return fmt.Errorf("promoção de %s: %w", o.promote, err)
		}
		fmt.Fprintf(stdout, "%s promovido a principal (época %d)\n", o.promote, epoch)
		return nil
	}

	cfg := newConfig(o, stdout)
	cfg.Logf("iniciando %s (dados em %s)", o.role, o.dataDir)
	if err := server.Run(ctx, cfg); err != nil {
		return err
	}
	cfg.Logf("encerrado")
	return nil
}
