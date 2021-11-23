#!/usr/bin/python
# coding: utf-8
import subprocess, os, time, threading
__author__ = 'Marcos Roberto - marcos.roberto@complexodopecem.com.br'
# Inicio variáveis globais
DESTINO_BACKUP = '/zimbra-backup/contas'
QUANT_THREADS = 12

# Método para gerar lista de emails existentes no servidor.
def get_lista_emails():
    CMD = ['/opt/zimbra/bin/zmprov', '-l', 'gaa']
    output = subprocess.Popen(CMD, stdout=subprocess.PIPE).communicate()[0].splitlines()
    output.sort()
    # Remover emails desnecessários
    output.remove('admin@cpdell202.cearaportosnet')
    output.remove('ham.lepb1uwwzl@cpdell202.cearaportosnet')
    output.remove('spam.8pgbjvxec@cpdell202.cearaportosnet')
    output.remove('virus-quarantine.cnq4ggbx@cpdell202.cearaportosnet')
    output.remove('galsync@complexodopecem.com.br')
    return output

#print get_lista_emails()
#print "\n".join(get_lista_emails())
#print len(get_lista_emails())

# Método para dividir a lista em X partes.
def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts]
             for i in range(wanted_parts) ]


# Método para criar lista de emails existentes no servidor.
def gerar_backup_conta(CONTA, path_bkp):
    nome_arquivo = '%s.tar.gz' % (CONTA)  # Cria o nome do arquivo de Backup./
    path_destino = '%s/%s' % (path_bkp, nome_arquivo)  # Destino onde será gravado o Backup
    CMD = '/opt/zimbra/bin/zmmailbox -z -m %s getRestURL -u http://127.0.0.1 "/?fmt=tgz" > %s' % (CONTA, path_destino)
    subprocess.call(CMD, shell=True)
    return


def inicio_backup_full(LISTA, PART):
    data_atual = time.strftime("%Y-%m-%d")
    PATH_BKP = '%s/%s/part-%s' % (DESTINO_BACKUP, data_atual, PART)
    if not os.path.isdir(PATH_BKP):
        os.makedirs(PATH_BKP)
    # Início

    # Realizar backup para cada email contido na lista
    for email in LISTA:
        print("Backup start:  %s" % email)
        gerar_backup_conta(email, PATH_BKP)
        print("Backup finish: %s" % email)

# Gerar dicionario dividindo a lista em 8 Partes
DICIONARIO = split_list(get_lista_emails(), wanted_parts=QUANT_THREADS)

#print "\n".join(DICIONARIO[0])

# Criando e iniciando as Threads
threads = []
for i in range(len(DICIONARIO)):
    t = threading.Thread(target=inicio_backup_full, args=[DICIONARIO[i], i])
    threads.append(t)
    t.start()

