package server

// Passo 7: reserva, promoção e época (Seções 6.1 e 6.2 do documento).
//
// A reserva sobe vazia e só responde HELLO (STATUS papel=reserva) e PROMOTE;
// o resto recebe NOT_LEADER. Quando o principal cai, o OPERADOR isola o
// principal antigo e manda PROMOTE para a reserva. Ela então:
//
//  1. escolhe uma época maior que todas as anteriores e grava no disco
//     (DataDir/epoch) ANTES de qualquer outra coisa;
//  2. abre um log novo e vazio para essa época (log-<época>.wal);
//  3. vira principal e abre a janela RECOVERING (30 s): aceita HELLO,
//     CREATE/RESOLVE_TOPIC e SUBSCRIBE, mas recusa PUBLISH com RECOVERING,
//     para dar tempo de os assinantes voltarem antes das leituras;
//  4. responde STATUS com papel=principal e a época nova.
//
// A época vai no STATUS. O cliente guarda a maior que viu e recusa um broker
// com época menor: é assim que ele não volta para um principal antigo.

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/GabriellCarvallho/mare-pubsub/broker/logrec"
	"github.com/GabriellCarvallho/mare-pubsub/protocol"
)

// DefaultRecovery é a janela RECOVERING depois da promoção (Seção 7.1).
const DefaultRecovery = 30 * time.Second

// Erros de Promote.
var (
	ErrAlreadyPrincipal = errors.New("o broker já é o principal")
	ErrUnreachable      = errors.New("broker inacessível")
)

// ---------------------------------------------------------------------------
// Época no disco

const epochFile = "epoch"

// readEpoch lê DataDir/epoch. found=false se o arquivo não existe ou se
// DataDir está vazio (época só na memória, usado nos testes).
func readEpoch(dir string) (epoch uint32, found bool, err error) {
	if dir == "" {
		return 0, false, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, epochFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 32)
	if err != nil || n == 0 {
		return 0, false, fmt.Errorf("arquivo de época inválido: %q", data)
	}
	return uint32(n), true, nil
}

// writeEpoch grava a época de forma ATÔMICA: escreve num arquivo
// temporário, faz Sync e renomeia. Se o broker cair no meio, fica o
// arquivo antigo inteiro ou o novo inteiro, nunca um pedaço.
func writeEpoch(dir string, epoch uint32) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, epochFile+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%d\n", epoch); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, epochFile)); err != nil {
		return err
	}
	// Sync da pasta: garante que o rename também chegou ao disco.
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// nextEpoch escolhe uma época maior que prev. Usa o relógio (segundos Unix)
// para que a reserva, que não conhece a época do principal antigo, ainda
// assim escolha um número maior que o dele (os relógios das máquinas são
// supostos razoavelmente sincronizados). Se o relógio estiver atrasado,
// prev+1 garante que a época nunca diminui na mesma máquina.
func nextEpoch(prev uint32, now time.Time) uint32 {
	u := now.Unix()
	if u > int64(prev) && u <= int64(^uint32(0)) {
		return uint32(u)
	}
	return prev + 1
}

// loadEpoch é chamado no Run, antes de abrir o log.
//   - principal: usa a época gravada (reinício com disco íntegro, mesmo
//     log); se não houver, cria uma nova e grava.
//   - reserva: só lê a época gravada (0 se não houver), para informar no
//     STATUS. Não cria nada: a reserva não grava antes de ser promovida.
func (b *broker) loadEpoch() error {
	epoch, found, err := readEpoch(b.cfg.DataDir)
	if err != nil {
		return err
	}
	if b.role == protocol.RolePrincipal && !found {
		epoch = nextEpoch(0, b.now())
		if err := writeEpoch(b.cfg.DataDir, epoch); err != nil {
			return fmt.Errorf("gravar época: %w", err)
		}
	}
	b.epoch = epoch
	return nil
}

// ---------------------------------------------------------------------------
// PROMOTE no broker

// promote transforma a reserva em principal. Resposta: STATUS com a época
// nova, ou ERROR (ALREADY_PRINCIPAL; STORAGE_UNAVAILABLE se o disco falhar,
// e então ela continua reserva e o operador pode tentar de novo).
func (b *broker) promote() protocol.Frame {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.role == protocol.RolePrincipal {
		return errFrame(protocol.CodeAlreadyPrincipal)
	}

	// 1. Época nova, gravada antes de tudo. Se cair depois daqui, ao
	//    reiniciar como principal ele usa esta época e este log.
	epoch := nextEpoch(b.epoch, b.now())
	if err := writeEpoch(b.cfg.DataDir, epoch); err != nil {
		b.cfg.Logf("promoção: gravar época: %v", err)
		return errFrame(protocol.CodeStorageUnavailable)
	}
	b.epoch = epoch

	// 2. Log novo, só desta época. A reserva começa sem tópicos nem
	//    publicações (Seção 6.2); o replay de um arquivo novo não lê nada.
	if err := b.openLog(); err != nil {
		b.cfg.Logf("promoção: abrir log: %v", err)
		return errFrame(protocol.CodeStorageUnavailable)
	}
	if err := b.appendRec(logrec.Record{Kind: logrec.KindEpoch, Epoch: epoch}); err != nil {
		b.log.Close()
		b.log = nil
		return errFrame(protocol.CodeStorageUnavailable)
	}

	// 3. Agora é principal, com a janela RECOVERING aberta.
	b.role = protocol.RolePrincipal
	b.recoveringUntil = b.now().Add(b.cfg.Recovery)
	b.cfg.Logf("promovido a principal: época %d, RECOVERING por %s", epoch, b.cfg.Recovery)

	return protocol.Frame{Type: protocol.TypeStatus, Role: b.role, Epoch: epoch}
}

// recovering diz se a janela RECOVERING ainda está aberta. Chamar com b.mu
// travado.
func (b *broker) recovering() bool {
	return b.now().Before(b.recoveringUntil)
}

// ---------------------------------------------------------------------------
// Promote: a ferramenta do operador

// promoteTimeout limita a conversa inteira com o broker.
const promoteTimeout = 5 * time.Second

// Promote conecta na reserva em addr, faz HELLO e manda PROMOTE. Devolve a
// época nova. Erros: ErrUnreachable (não conectou ou a conexão caiu),
// ErrAlreadyPrincipal, ou outro erro se o broker recusar.
//
// ATENÇÃO (Seção 6.1): antes de chamar, o operador precisa isolar o
// principal antigo (desligar, tirar da rede). A época ajuda os clientes, mas
// não impede dois principais ao mesmo tempo.
func Promote(addr string) (uint32, error) {
	conn, err := net.DialTimeout("tcp", addr, promoteTimeout)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(promoteTimeout))

	// client_id 0: o operador não é sensor nem assinante.
	if err := protocol.WriteFrame(conn, protocol.Frame{Type: protocol.TypeHello, Version: protocol.Version}); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	if f.Type != protocol.TypeStatus {
		return 0, fmt.Errorf("resposta inesperada ao HELLO: %v", f)
	}

	if err := protocol.WriteFrame(conn, protocol.Frame{Type: protocol.TypePromote}); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	f, err = protocol.ReadFrame(conn)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	switch {
	case f.Type == protocol.TypeStatus && f.Role == protocol.RolePrincipal:
		return f.Epoch, nil
	case f.Type == protocol.TypeError && f.ErrCode == protocol.CodeAlreadyPrincipal:
		return 0, ErrAlreadyPrincipal
	case f.Type == protocol.TypeError:
		return 0, fmt.Errorf("promoção recusada: %s", protocol.CodeName(f.ErrCode))
	}
	return 0, fmt.Errorf("resposta inesperada ao PROMOTE: %v", f)
}
