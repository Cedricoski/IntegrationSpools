import json
import os
import shutil
import subprocess
import time
import datetime
import logging


class IntegrationSpool:
    F = open('config.json')
    TODAY = datetime.datetime.now().strftime('%Y%M%d')
    DATAS = json.load(F)
    F.close()
    if not os.path.exists('IntegrationSpool.log'):
        f = open('IntegrationSpool.log', 'a')
        f.close()
    FORMAT = '%(asctime)s %(levelname)s : %(message)s'
    logging.basicConfig(format=FORMAT, level="INFO", filename='IntegrationSpool.log')

    def __init__(self):
        self.dir_get = ''
        self.base_name = self.DATAS['BASENAME']
        self.user = self.DATAS['USER']
        self.password = self.DATAS['PASSWORD']
        self.user_admin = self.DATAS['USERADMIN']
        self.password_admin = self.DATAS['PASSWORDADMIN']
        self.frm_name = ''
        self.frm_dir = self.DATAS['FRMDIR']
        self.fic = self.DATAS['FIC']
        self.root = self.DATAS['ROOT']
        self.client_dir = self.DATAS['CLIENT']
        self.work_dir = f'{self.root}\\WORK\\'
        self.root_batch = self.DATAS['ROOTBATCH']
        self.transfer = ''
        self.sav_dir = ''
        self.err_dir = f'{self.root}\\STAT\\KO'
        self.log = f'{self.root}LOG'
        self.cache_dir = self.DATAS['CACHEDIR']
        self.bin_dir = self.DATAS['BINDIR']
        self.srv = self.DATAS['SRV']
        self.port = self.DATAS['PORT']
        self.filename_without_ext_path = ''
        self.filename_without_ext = ''

        self.index()
    def build(self):
        if not os.path.exists(self.root):
            os.makedirs(self.root)

        if not os.path.exists(self.work_dir):
            os.makedirs(self.work_dir)

        if not os.path.exists(self.root_batch):
            os.makedirs(self.root_batch)

        if not os.path.exists(self.transfer):
            os.makedirs(self.transfer)

        if not os.path.exists(self.sav_dir):
            os.makedirs(self.sav_dir)

        if not os.path.exists(self.client_dir):
            os.makedirs(self.client_dir)

        if not os.path.exists(self.frm_dir):
            os.makedirs(self.frm_dir)

        if not os.path.exists(self.err_dir):
            os.makedirs(self.err_dir)

        if not os.path.exists(self.log):
            os.makedirs(self.log)

        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def index(self):

        for base in self.base_name:
            if self.CheckBase(base):
                self.dir_get = self.frm_name = self.fic = base
                self.transfer = f'{self.root_batch}\\{self.dir_get}\\'
                self.sav_dir = f'{self.root}\\{self.dir_get}\\SAV\\'
                self.build()
                # Vérification du nom et de l'extension du fichier pour déplacement dans le work
                transfer_list = os.listdir(self.transfer)
                for file in transfer_list:
                    if self.fic in file and file.endswith('pdf'):
                        shutil.move(self.transfer + file, self.work_dir)
                    else:
                        continue

                self.Run(base)
            else:
                logging.error(f'Base {base} non démarrée')
                continue

    def CheckBase(self, base):
        if os.path.isfile(f'{self.cache_dir}{base}.txt'):
            os.remove(f'{self.cache_dir}{base}.txt')
        cmd = f'{self.bin_dir}amf-basesCtrl.bat status -login {self.user_admin} -password {self.password_admin} -domain ALL -h {self.srv} -p {self.port} -base {base} >{self.cache_dir}{base}.txt'
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if os.path.isfile(f'{self.cache_dir}{base}.txt'):
            f = open(f'{self.cache_dir}{base}.txt', 'r')
            for line in f:
                if line == 'started':
                    return True
        return False

    def Run(self, base):
        dir = os.listdir(self.work_dir)
        for file in dir:
            # Vérification du nom et de l'extension du fichier
            if self.fic in file and file.endswith('pdf'):
                self.filename_without_ext_path = os.path.splitext(self.work_dir + file)[0]
                self.filename_without_ext = os.path.basename(self.filename_without_ext_path)
                os.makedirs(self.filename_without_ext_path)

            if os.path.exists(self.client_dir + 'dbaet.trc'):
                os.remove(self.client_dir + 'dbaet.trc')

            # Exécution des commandes dbPDF et dbAet
            dbPdf_cmd = f'{self.work_dir}DBPDF.EXE /DIR={self.filename_without_ext_path}\\ /SPOOL={self.work_dir}{file} /OUT={self.filename_without_ext_path}\\{self.filename_without_ext}.LST /SEP='
            dbAet_cmd = f'{self.work_dir}DBAETT.EXE /LST={self.filename_without_ext_path}\\{self.filename_without_ext}.LST /FRM={self.frm_dir}\\{self.frm_name}.FRM /USER={self.user} /PASS={self.password} /BASEID={base} ERRDIR={self.err_dir} /PACK=ZLIB /TRACE /DELEXTDOC /END'

            res_dbPdf = subprocess.run(dbPdf_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(5)
            res_dbAet = subprocess.run(dbAet_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.ErrorSav(file)

    def ErrorSav(self, file):
        if os.path.isfile(self.client_dir + 'dbaet.trc'):
            dbaet_trc = open(self.client_dir + 'dbaet.trc', 'r')

            if not os.path.isfile(f'{self.log}\\{self.filename_without_ext}_REJ.txt'):
                file_rej = f'{self.log}\\{self.filename_without_ext}_REJ.txt'
                f = open(file_rej)
                f.close()
            if not os.path.isfile(f'{self.log}\\{self.filename_without_ext}_ACC.txt'):
                file_acc = f'{self.log}\\{self.filename_without_ext}_ACC.txt'
                f = open(file_acc)
                f.close()
            for line in dbaet_trc:
                if 'Nombre de pages rejet' in line:
                    nb_rej = int(line.split(':')[1].lstrip().split(" ")[0])
                    f = open(file_rej, 'a')
                    f.write(line)
                    f.close()
                if 'Nombre de pages accept' in line:
                    nb_acc = int(line.split(':')[1].lstrip())
                    f = open(file_acc, 'a')
                    f.write(line)
                    f.close()
            if nb_rej != 0:
                self.HandleError(file, dbaet_trc)
            elif nb_acc == 0:
                self.HandleError(file, dbaet_trc)
            else:
                self.HandleSuccess(file, dbaet_trc)
            dbaet_trc.close()

    def HandleError(self, file, dbaet_trc):
        if not os.path.exists(f'{self.sav_dir}{self.TODAY}'):
            os.makedirs(f'{self.sav_dir}{self.TODAY}')

        shutil.copy(f'{self.work_dir}{file}', f'{self.sav_dir}{self.TODAY}')

        if not os.path.isfile(f'{self.log}\\KO_{self.filename_without_ext}_{self.TODAY}.txt'):
            f = open(f'{self.log}\\KO_{self.filename_without_ext}_{self.TODAY}.txt')
            f.close()

        f = open(f'{self.log}\\KO_{self.filename_without_ext}_{self.TODAY}.txt', 'a')
        f.write(f'ERREUR ARCHIVAGE {self.filename_without_ext}\n')

        for line in dbaet_trc:
            f.write(line + '\n')
        f.close()
        if os.path.isfile(f'{self.filename_without_ext_path}\\{self.filename_without_ext}.LST'):
            shutil.copy(f'{self.filename_without_ext_path}\\{self.filename_without_ext}.LST', f'{self.err_dir}')
        os.remove(f'{self.work_dir}{file}')
        os.remove(f'{self.log}\\{self.filename_without_ext}_REJ.txt')
        os.remove(f'{self.log}\\{self.filename_without_ext}_ACC.txt')
        os.rmdir(self.filename_without_ext_path)

    def HandleSuccess(self, file, dbaet_trc):
        if not os.path.isfile(f'{self.log}\\OK_{self.filename_without_ext}_{self.TODAY}.txt'):
            f = open(f'{self.log}\\OK_{self.filename_without_ext}_{self.TODAY}.txt')
            f.close()

        f = open(f'{self.log}\\OK_{self.filename_without_ext}_{self.TODAY}.txt', 'a')
        f.write(f'ARCHIVAGE OK {self.filename_without_ext}\n')

        for line in dbaet_trc:
            f.write(line + '\n')
        f.close()
        os.remove(f'{self.work_dir}{file}')
        os.remove(f'{self.log}\\{self.filename_without_ext}_REJ.txt')
        os.remove(f'{self.log}\\{self.filename_without_ext}_ACC.txt')
        os.rmdir(self.filename_without_ext_path)
