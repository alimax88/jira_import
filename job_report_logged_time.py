#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jira
import re
import smtplib
import cx_Oracle
import unicodedata
import calendar

import jira_config

from email.mime.text import MIMEText
from datetime import date, datetime

logger_actived = True

class JiraTsReporter:
    def __init__(self):
        # Dati per la connessione a JIRA
        jira_user = jira_config.jira_user
        jira_psw = jira_config.jira_psw
        self.jira_client = jira.JIRA(options={'server': jira_config.jira_server}, basic_auth=(jira_user, jira_psw))

        # Connessione al DB
        connection_string = jira_config.connection_string
        self.connection = cx_Oracle.connect(connection_string)

        # Dati per l'invio della mail con il report degli errori
        self.from_email = jira_config.from_email
        self.email_password = jira_config.email_password
        self.to_email = jira_config.to_email

        # Variabili generali
        self.applicazioni = jira_config.applicazioni
        self.mapping_fasi_am = jira_config.mapping_fasi_am
        self.issue_am = jira_config.issue_am
        self.year = 0
        self.month = 0
        self.no_epic = {}
        self.wrong_log = {}
        self.work_log = {}
        self.ore_decimali = {}
        self.log_error = {}
        self.fasi_recuperate = []
        self.fasi_dipendente = []
        self.email_dip = ''

    def close(self):
        self.connection.close()

    def output_logger(self, string):
        if logger_actived:
            print string

    def report_error(self, error_msg):
        """
            Riporta gli errori incrontrati nelle query.

            :param error_msg: messaggio di errore
        """
        if self.email_dip not in self.log_error:
            self.log_error[self.email_dip] = [error_msg]
        else:
            self.log_error[self.email_dip].append(error_msg)

    def clean_database(self, id_dips, all_dip=False):
        """
            Permette di pulire il database andando a cancellare tutti i ticket del mese riguardanti fasi con
            applicazioni di JIRA per una lista di dipendenti o per tutti se e' specificato 'all_dip'
        """
        cursor = self.connection.cursor()

        # Recupero anno, mese e ultimo giorno del mese necessari per le query
        if date.today().day <= 10:
            if date.today().month == 1:
                month = 12
                year = date.today().year - 1
            else:
                month = date.today().month - 1
                year = date.today().year
        else:
            month = date.today().month
            year = date.today().year

        day_range = calendar.monthrange(year, month)
        last_day = day_range[1]

        self.output_logger("Periodo di cancellazione:\nAnno {0}\nMese {1}".format(year, month))

        # Costruisco la where per gli id dei dipendenti
        where = ''
        if id_dips:
            where = "AND dt.id_dipendente IN ({id_dips})".format(id_dips=(', '.join(map(str, id_dips))))
        if all_dip:
            # Recupero gli id di tutti i dipendenti
            id_dips = []
            query = """
                SELECT id_dipendente
                FROM dipendente
                WHERE data_finerapporto is NULL
            """
            try:
                result = cursor.execute(query)
                for row in result.fetchall():
                    id_dips.append(row[0])
            except cx_Oracle.DatabaseError as e:
                print '\tQuery per ricavare id dipendenti non riuscita:\n\t\t{0}\n\t\t{1}'.format(query, e)
                cursor.close()
            where = "AND dt.id_dipendente IN ({id_dips})".format(id_dips=(', '.join(map(str, id_dips))))

        # Recupero la lista dei id_ticket associati
        ticket_list = []
        query = """
            SELECT t.id_ticket
            FROM ticket t
            JOIN day_tracking dt ON dt.id_day_tracking = t.id_day_tracking
            WHERE dt.giorno >= TO_DATE('{year}-{month}-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            AND dt.giorno <= TO_DATE('{year}-{month}-{last_day} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            {where}
            """.format(where=where, year=year, month=month, last_day=last_day)
        try:
            result = cursor.execute(query)
            for row in result.fetchall():
                ticket_list.append(row[0])
        except cx_Oracle.DatabaseError as e:
            print '\tQuery per ricavare id ticket non riuscita:\n\t\t{0}\n\t\t{1}'.format(query, e)
            ticket_list = []

        # Recupero la lista dei id_day_tracking associati
        day_tracking_list = []
        query = """
            SELECT dt.id_day_tracking
            from day_tracking dt
            WHERE dt.giorno >= TO_DATE('{year}-{month}-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            AND dt.giorno <= TO_DATE('{year}-{month}-{last_day} 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            {where}
        """.format(where=where, year=year, month=month, last_day=last_day)
        try:
            result = cursor.execute(query)
            for row in result.fetchall():
                day_tracking_list.append(row[0])
        except cx_Oracle.DatabaseError as e:
            print '\tQuery per ricavare id day tracking non riuscita:\n\t\t{0}\n\t\t{1}'.format(query, e)
            day_tracking_list = []

        print ticket_list
        print len(ticket_list)
        print day_tracking_list
        print len(day_tracking_list)

        # Elimino i ticket trovati
        if ticket_list:
            for count in range(0, (len(ticket_list)/999)+1):
                if len(ticket_list[count*1000:]) > 999:
                    query = "DELETE FROM ticket WHERE id_ticket IN ({ticket_list})".format(ticket_list=(', '.join(map(str, ticket_list[count*1000:(count+1)*1000]))))
                else:
                    query = "DELETE FROM ticket WHERE id_ticket IN ({ticket_list})".format(ticket_list=(', '.join(map(str, ticket_list[count*1000:]))))
                try:
                    cursor.execute(query)
                    self.connection.commit()
                except cx_Oracle.DatabaseError as e:
                    print '\tQuery per eliminare id ticket non riuscita:\n\t\t{0}\n\t\t{1}'.format(query, e)

        # Elimino i day tracking trovati
        if day_tracking_list:
            for count in range(0, (len(day_tracking_list)/999)+1):
                if len(ticket_list[count*1000:]) > 999:
                    query = "DELETE FROM day_tracking WHERE id_day_tracking IN ({day_tracking_list})".format(day_tracking_list=(', '.join(map(str, day_tracking_list[count*1000:(count+1)*1000]))))
                else:
                    query = "DELETE FROM day_tracking WHERE id_day_tracking IN ({day_tracking_list})".format(day_tracking_list=(', '.join(map(str, day_tracking_list[count*1000:]))))
                try:
                    cursor.execute(query)
                    self.connection.commit()
                except cx_Oracle.DatabaseError as e:
                    print '\tQuery per eliminare id day_tracking non riuscita:\n\t\t{0}\n\t\t{1}'.format(query, e)

        cursor.close()

    def check_status(self, tab, id_tab, id_dipendente):
        """
            Metodo che controlla lo stato di una fase o di una commessa in modo che sia associata ad un dipendente e
            che sia visibile sul Timesheet OT.
            Per rendere visibile una fase o una commessa e' necessario che la data odierna sia compresa tra i rispettivi
            campi 'data_inizio' e 'data_fine'.
            Il rinnovo viene fatto su base annuale quindi in caso di insert o update, la 'data_fine' sara' impostata
            sul 31 Dicembre dell'anno in corso (caso speciale e' dall'1 al 10 di Gennaio siccome sta valutando ancora
            l'anno precedente).
        """
        cursor = self.connection.cursor()
        data_found = False
        try:
            # Tento di recuperare l'associazione se gia' presente
            result = cursor.execute("""
                SELECT data_fine
                FROM {0}_dipendente
                WHERE id_dipendente = {1}
                AND id_{0} = {2}
                """.format(tab, id_dipendente, id_tab))
            for row in result.fetchall():
                data_found = True
                # Associazione trovata, controllo se devo eseguire l'update della data di fine
                data_fine = row[0]
                if data_fine != "{0}-12-31 00:00:00".format(self.year):
                    try:
                        cursor.execute("""
                            UPDATE {0}_dipendente
                            SET data_fine = TO_DATE('{1}-12-31 00:00:00', 'YYYY-MM-DD HH24-MI-SS')
                            WHERE id_{0} = {2}
                            """.format(tab, self.year, id_tab))
                        self.connection.commit()
                    except cx_Oracle.DatabaseError as e:
                        error_msg = 'Aggiornamento della data di fine per la {0} {1} associata a id_dipendente {2} non riuscita: {3}'.format(tab, id_tab, id_dipendente, e)
                        self.report_error(error_msg)
        except:
            data_found = False

        if not data_found:
            # Associazione non trovata, la aggiungo.
            try:
                cursor.execute("""
                    INSERT INTO {0}_dipendente (id_dipendente, id_{0}, data_inizio, data_fine)
                    VALUES({1}, {2}, TO_DATE('{3}-01-01 00:00:00', 'YYYY-MM-DD HH24-MI-SS'), TO_DATE('{3}-12-31 00:00:00', 'YYYY-MM-DD HH24-MI-SS'))
                    """.format(tab, id_dipendente, id_tab, self.year))
                self.connection.commit()
            except cx_Oracle.DatabaseError as e:
                error_msg = 'Associazione tra {0} {1} e id_dipendente {2} non riuscita: {3}'.format(tab, id_tab, id_dipendente, e)
                self.report_error(error_msg)
        cursor.close()

    def add_id(self, insert, select):
        """
            Esegue le query di insert per la creazione di un nuovo record.
        """
        id_inserted = -1
        cursor = self.connection.cursor()
        try:
            cursor.execute(insert)
            self.connection.commit()
            result = cursor.execute(select)
            for row in result.fetchall():
                id_inserted = row[0]
        except:
            id_inserted = -1
        cursor.close()
        return id_inserted

    def get_id(self, query):
        """
            Esegue le query di select per la restituzione di un id.
        """
        id_found = -1
        cursor = self.connection.cursor()
        try:
            result = cursor.execute(query)
            for row in result.fetchall():
                id_found = row[0]
        except cx_Oracle.DatabaseError as e:
            error_msg = '\tQuery per ricavare id non riuscita:\n\t\t{0}\n\t\t{1}'.format(query, e)
            self.report_error(error_msg)
            id_found = -1
        cursor.close()
        return id_found

    def consuntiva_commessa(self, id_commessa):
        """
            Se non e' gia' presente, esegue la query per inserire la commessa nella tabella stampa_commessa in modo che
            possa essere visibile nel tab "Gestione consuntivazione".
        """
        cursor = self.connection.cursor()
        try:
            result = cursor.execute("""
                SELECT id_commessa FROM stampa_commessa WHERE id_commessa = {0}
            """.format(id_commessa))
            for row in result.fetchall():
                if row[0]:
                    cursor.close()
                    return
        except cx_Oracle.DatabaseError as e:
            error_msg = '\tRicerca id_commessa {0} nella tabella STAMPA_COMMESSA non riuscita.\n\t\t{1}'.format(id_commessa, e)
            self.report_error(error_msg)
            cursor.close()
            return

        # try:
        #     cursor.execute("""
        #             INSERT INTO stampa_commessa (id_commessa, id_tipo_stampa)
        #             VALUES ({0}, 3)
        #         """.format(id_commessa))
        #     self.connection.commit()
        # except cx_Oracle.DatabaseError as e:
        #     error_msg = '\tInserimento id_commessa {0} nella tabella STAMPA_COMMESSA non riuscita.\n\t\t{1}'.format(id_commessa, e)
        #     self.report_error(error_msg)
        cursor.close()

    def delete_tickets(self, ticket_list=''):
        """
            Elimina dal DB i ticket che non sono stati attraversati durante l'esecuzione dell'algoritmo ad eccezzione di
            quelli che non rientrano tra le fasi registrate su JIRA.
        """
        if not ticket_list:
            return

        cursor = self.connection.cursor()
        try:
            cursor.execute("DELETE FROM ticket WHERE id_ticket IN ({0})".format(ticket_list))
            self.connection.commit()
            self.output_logger("\n\n////////////////////////////\nEliminati i ticket {0}\n////////////////////////////".format(ticket_list))
        except cx_Oracle.DatabaseError as e:
            error_msg = '\tQuery per eliminare id_ticket non riuscita:\n\t\t({0})\n\t\t{1}'.format(ticket_list, e)
            self.report_error(error_msg)
        cursor.close()

    def delete_tickets_by_fasi(self, id_dipendente):
        """
            Recupera tutti gli id dei ticket associati ad un dipendente e le sue fasi non parsate dall'algoritmo.
        """
        # In fasi_recuperate sono rimaste solo le fasi che l'algoritmo non ha parsato
        fasi = ', '.join(map(str, self.fasi_recuperate))

        cursor = self.connection.cursor()
        tickets = []
        try:
            result = cursor.execute("""
                SELECT distinct t.id_ticket
                FROM ticket t
                JOIN day_tracking d ON d.id_day_tracking = t.id_day_tracking
                WHERE d.id_dipendente = {0}
                AND t.id_fase IN ({1})
                AND EXTRACT(year FROM d.giorno) = {2}
                AND EXTRACT(month FROM d.giorno) = {3}
                """.format(id_dipendente, fasi, self.year, self.month))
            for row in result.fetchall():
                tickets.append(row[0])
        except cx_Oracle.DatabaseError as e:
            error_msg = '\tQuery per ricavare gli id_ticket associati a id_dipendente {0} per le fasi ({1}) non riuscita:\n\t\t{2}\n'.format(id_dipendente, fasi, e)
            self.report_error(error_msg)
        cursor.close()

        ticket_list = ', '.join(map(str, tickets))
        if ticket_list:
            self.delete_tickets(ticket_list)

    def fetch_all_fasi(self, id_dipendente):
        """
            Recupera tutti gli id delle fasi associate ad un dipendente.
            Vengono prese solo quelle appartenenti ad applicazioni con codice utilizzato su Jira.
        """
        cursor = self.connection.cursor()
        self.fasi_recuperate = []
        try:
            result = cursor.execute("""
                SELECT distinct t.id_fase
                FROM ticket t
                JOIN day_tracking d ON d.id_day_tracking = t.id_day_tracking
                JOIN fase f ON f.id_fase = t.id_fase
                JOIN commessa c ON c.id_commessa = f.id_commessa
                JOIN applicazione a ON a.id_applicazione = c.id_applicazione
                WHERE d.id_dipendente = {0}
                AND EXTRACT(year FROM d.giorno) = {1}
                AND EXTRACT(month FROM d.giorno) = {2}
                AND a.descrizione IN ('{3}')
                """.format(id_dipendente, self.year, self.month, "', '".join(self.applicazioni)))
            for row in result.fetchall():
                self.fasi_recuperate.append(row[0])
        except cx_Oracle.DatabaseError as e:
            error_msg = '\tQuery per ricavare gli id_fase associati a id_dipendente {0} non riuscita:\n\t\t{1}\n'.format(id_dipendente, e)
            self.report_error(error_msg)
        cursor.close()

    def is_saturday(self, log_date):
        """
            Controlla se il giorno di log e' un sabato.
        """
        data = log_date.split()[0]
        giorno = datetime.strptime(data, '%Y-%m-%d').strftime('%A')

        if giorno == 'Saturday':
            return True
        return False

    def ricalcola_ore(self, ore_loggate, sum_ore_lavoro):
        """
            Ricalcola le ore lavorate e quelle di straordinario da assegnare per un determinato giorno in base a quelle
            gia' inserite in altri ticket.
        """
        if sum_ore_lavoro + ore_loggate <= 8:
            ore_lavoro = ore_loggate
            ore_straordinari = 0
        else:
            ore_lavoro = 8 - sum_ore_lavoro
            ore_straordinari = ore_loggate - 8 + sum_ore_lavoro
        return ore_lavoro, ore_straordinari

    def ticketing(self, log_date, id_day_tracking, id_fase, log):
        """
            Per prima cosa viene ricavata la somma delle ore di lavoro (non straordinari) sulle fasi per un
            id_day_tracking ad esclusione di quella attualmente in valutazione.
            In seguito viene ricavato l'id_ticket e le ore di lavoro e straordinari per la fase attuale.
            In caso venga trovato un risultato per quest'ultima query si procede ad una update nei casi in cui la somma
            tra le ore di lavoro e quelle di straordinari differiscono dalle ore loggate oppure se la somma tra il
            totale delle ore loggate nel giorno piu' quelle della fase attuale ecceda le 8h.
            Se invece non viene trovato un id_ticket si procede ad una insert.
        """
        # Ricavo la somma delle ore gia' lavorate durante una giornata da un dipendente escludendo eventualmente gli
        # id_day_tracking associati all'id_fase dati in input
        cursor = self.connection.cursor()
        result = cursor.execute("""
            SELECT SUM(ore_lavoro)
            FROM ticket
            WHERE id_day_tracking = {0}
            AND NOT id_fase = {1}
            """.format(id_day_tracking, id_fase))
        sum_ore_lavoro = -1
        for row in result.fetchall():
            sum_ore_lavoro = row[0]
        if sum_ore_lavoro == -1 or not sum_ore_lavoro:
            sum_ore_lavoro = 0
        self.output_logger('Sommma ore lavoro: {0}'.format(sum_ore_lavoro))

        # Cerco se esiste gia' un ticket per il giorno lavorativo associato alla fase in valutazione per ricavarne l'id
        # e le ore precedentemente inserite.
        result = cursor.execute("""
            SELECT id_ticket, ore_lavoro, ore_straordinari
            FROM ticket
            WHERE id_day_tracking = {0}
            AND id_fase = {1}
            """.format(id_day_tracking, id_fase))
        ticket_found = False
        for row in result.fetchall():
            # Ticket trovato.
            ticket_found = True
            id_ticket = row[0]
            old_ore_lavoro = row[1]
            old_ore_straordinari = row[2]
            self.output_logger('Id ticket {0} contiene {1} ore di lavoro e {2} di straordinari'.format(id_ticket, old_ore_lavoro, old_ore_straordinari))

            # Rimuovo la fase trovata dalla lista di quelli del dipendente se e' presente
            if id_fase in self.fasi_recuperate:
                self.fasi_recuperate.remove(id_fase)

            if old_ore_lavoro + old_ore_straordinari != log['log_time'] or sum_ore_lavoro + old_ore_lavoro != 8:
                # Se le ore non combaciano oppure sono state eseguite modifiche su altri ticket tali per cui le ore di
                # lavoro sforano le 8 ore, si esegue un update.
                if self.is_saturday(log_date):
                    ore_lavoro = 0
                    ore_straordinari = log['log_time']
                else:
                    ore_lavoro, ore_straordinari = self.ricalcola_ore(log['log_time'], sum_ore_lavoro)

                try:
                    cursor.execute("""
                        UPDATE ticket
                        SET ore_lavoro = {0}, ore_straordinari = {1}
                        WHERE id_ticket = {2}
                        """.format(ore_lavoro, ore_straordinari, id_ticket))
                    self.connection.commit()
                except cx_Oracle.DatabaseError as e:
                    cursor.close()
                    error_msg = '\tUpdate del id_ticket {0} con id_day_tracking {1} e id_fase {2} per il giorno {3} non riuscita: {4}'.format(id_ticket, id_day_tracking, id_fase, log_date, e)
                    self.report_error(error_msg)
                    return
                self.output_logger('Ticket aggiornato a {0} ore di lavoro e {1} di straordinari'.format(ore_lavoro, ore_straordinari))

        if not ticket_found:
            # Ticket non trovato, eseguo un'insert.
            if self.is_saturday(log_date):
                ore_lavoro = 0
                ore_straordinari = log['log_time']
            else:
                ore_lavoro, ore_straordinari = self.ricalcola_ore(log['log_time'], sum_ore_lavoro)

            next_val = self.get_id('SELECT seq_ticket.nextval from dual')
            try:
                cursor.execute("""
                    INSERT INTO ticket (id_stato_validazione, id_ticket, id_day_tracking, id_fase, ore_lavoro, ore_straordinari)
                    VALUES (1, {0}, {1}, {2}, {3}, {4})
                    """.format(next_val, id_day_tracking, id_fase, ore_lavoro, ore_straordinari))
                self.connection.commit()
            except cx_Oracle.DatabaseError as e:
                cursor.close()
                error_msg = '\tCreazione del ticket con id_day_tracking {0} e id_fase {1} per il giorno {2} non riuscita.\n\t\t\tOre da loggare: {3}\n\t\t\tStraordinari da loggare: {4}\n\t\t\t{5}'.format(id_day_tracking, id_fase, log_date, ore_lavoro, ore_straordinari, e)
                self.report_error(error_msg)
                return
            self.output_logger('*** CREATO Id ticket {0} con {1} ore di lavoro e {2} di straordinari'.format(next_val, ore_lavoro, ore_straordinari))
        cursor.close()

    def write_log(self):
        """
            Vengono eseguite le query per rendicontare le ore inserite nel timesheet JIRA in quello di OT.
            Per ciascun dipendente che ha loggato viene recuperato il suo id. Per ogni sua loggata vengono estrapolati
            prima gli id del cliente e dell'applicazione, in secondo luogo quello della commessa e infine la fase.
            In tutti questi casi se avviene un'errore durante le query, esso viene riportato in un dizionario che come
            chiavi ha le email dei dipendenti e come valore il messaggio di errore.
            A differenza degli altri, durante la ricerca della commessa prima si cerca il codice_ot inserito nella issue
            di tipo epic e se non viene trovato si ritenta usando il codice di default. Se viene trovato quest'ultimo ed
            e' presente un codice_ot viene eseguito l'update.
            Se tutte le query sono andate a buon fine si procede con il metodo che gestisce il ticketing.
            Se dei worklog precedentemente riportati nel DB vengono cancellati su Jira e' necessario eliminarli. Per
            fare cio', per ogni dipendente viene recuperata la lista di ticket ad esso associati in un determinato mese.
            Man mano che l'algoritmo procede vengono eliminati dalla lista gli id_ticket trovati. Infine i rimanenti
            indicheranno quelli che saranno eliminati dal DB.
        """
        day_range = calendar.monthrange(self.year, self.month)

        for email_dip, date_list in self.work_log.iteritems():
            error_am_dip = False
            self.email_dip = email_dip
            id_dipendente = self.get_id("""
                SELECT id_dipendente
                FROM dipendente
                WHERE id='{0}'
            """.format(self.email_dip))
            if id_dipendente == -1:
                self.log_error[self.email_dip] = ['Dipendente non trovato in database']
                continue
            self.output_logger('\n\n*********** Dipendente {0} con id {1}:\n\n'.format(email_dip, id_dipendente))

            # Recupero la lista di tuttle fasi di un dipendente e quelle che vengono attraversate dall'algoritmo
            # verranno eliminate da tale lista. Al termine, le rimanenti vengono eliminati dal DB.
            self.fetch_all_fasi(id_dipendente)

            for day_num in range(1, day_range[1] + 1):
                log_date = '{year}-{month}-{day}'.format(day=str(day_num).rjust(2,'0'), month=str(self.month).rjust(2,'0'), year=self.year)
                log_list = date_list[log_date]
                self.output_logger('\n\n------------------ Data {0} ----------------'.format(log_date))
                for log in log_list:
                    # Controllo l'esistenza del cliente. In caso di mancanza viene avviata la segnalazione.
                    id_cliente = self.get_id("""
                        SELECT id_cliente
                        FROM cliente
                        WHERE ragione_sociale='{0}'
                    """.format(log['cliente']))
                    if id_cliente == -1 and log['cliente'] not in self.log_error:
                        self.log_error[log['cliente']] = 'Cliente non trovato in database'
                        continue

                    # Controllo l'esistenza dell'applicazione. In caso di mancanza viene avviata la segnalazione.
                    id_applicazione = self.get_id("""
                        SELECT id_applicazione
                        FROM applicazione
                        WHERE descrizione='{0}'
                    """.format(log['applicazione']))
                    if id_applicazione == -1 and log['applicazione'] not in self.log_error:
                        self.log_error[log['applicazione']] = 'Applicazione non trovata in database'
                        continue
                    self.output_logger('\nCliente {0} id {1} ---> Applicazione {2} id {3}'.format(log['cliente'], id_cliente, log['applicazione'], id_applicazione))

                    # Controllo l'esistenza della commessa. In caso di mancanza vine cercata se esiste un'altra commessa
                    # in cui e' registrato il codice 'segnaposto <project_key><id_issue_epic>.
                    # Se esiste viene eseguito l'update del codice altrimenti creato un nuovo record e in caso di errore
                    # viene avviata la segnalazione.
                    select = ("""
                        SELECT id_commessa
                        FROM commessa
                        WHERE codice_ot = '{0}'
                        AND id_cliente = {1}
                        AND id_applicazione = {2}
                    """.format(log['commessa'], id_cliente, id_applicazione))
                    if log['commessa']:
                        commessa = log["commessa"]
                        id_commessa = self.get_id(select)
                        if id_commessa != -1:
                            self.output_logger('Commessa trovata ---> {0} con codice {1}'.format(id_commessa, commessa))
                    else:
                        commessa = log["commessa_default"]
                        id_commessa = -1

                    if id_commessa == -1:
                        # Cerco la commessa usando il codice di default
                        select_default = ("""
                            SELECT id_commessa
                            FROM commessa
                            WHERE codice_ot = '{0}'
                            AND id_cliente = {1}
                            AND id_applicazione = {2}
                        """.format(log['commessa_default'], id_cliente, id_applicazione))
                        id_commessa = self.get_id(select_default)

                        if id_commessa != -1 and log['commessa']:
                            # Eseguo l'update del codice della commessa
                            cursor = self.connection.cursor()
                            try:
                                cursor.execute("""
                                    UPDATE commessa
                                    SET codice_ot = '{0}'
                                    WHERE id_commessa = {1}
                                """.format(commessa, id_commessa))
                                self.connection.commit()
                            except cx_Oracle.DatabaseError as e:
                                error_msg = '\tUpdate del codice_ot per id_commessa {0} in {1} non riuscita: {2}'.format(id_commessa, log['commessa'], e)
                                self.report_error(error_msg)
                                cursor.close()
                                continue
                            self.output_logger('Commessa default trovata e aggiornata ---> {0} con codice da {1} a {2}'.format(id_commessa, log['commessa_default'], commessa))
                            cursor.close()

                        elif id_commessa == -1:
                            # Eseguo l'inserimento della commessa
                            next_val = self.get_id('SELECT seq_commessa.nextval from dual')
                            insert = ("""
                                INSERT INTO commessa (id_commessa, id_tipo_commessa, codice_ot, id_stato_commessa, id_applicazione, descrizione, id_cliente, annocompetenza)
                                VALUES ({0}, 1, '{1}', 1, {2}, '{3}', {4}, {5})
                            """.format(next_val, commessa, id_applicazione, log['descrizione'], id_cliente, self.year))

                            if log['commessa']:
                                id_commessa = self.add_id(insert, select)
                            else:
                                id_commessa = self.add_id(insert, select_default)

                            if id_commessa == -1:
                                error_msg = '\tCreazione commessa {0} di applicazione {1} non riuscita.'.format(commessa, log['applicazione'])
                                self.report_error(error_msg)
                                continue
                            self.output_logger('Commessa {0} creata'.format(id_commessa))

                        else:
                            self.output_logger('Commessa default trovata e no update---> {0} con codice {1}'.format(id_commessa, commessa))

                    # Rendo la commessa visibile nella sezione "Gestione Consuntivazione". Di default metto
                    # id_tipo_stanpa = 3
                    self.consuntiva_commessa(id_commessa)

                    # Controllo che la commessa sia visibile al dipendente
                    self.check_status("commessa", id_commessa, id_dipendente)

                    # Controllo l'esistenza della fase. In caso di mancanza vine creato un nuovo record e in caso
                    # di errore viene avviata la segnalazione.
                    # Per la commessa dell'AM MMFG la fase viene cablata.
                    if 'AM MMFG' in log['fase']:
                        id_fase = self.mapping_fasi_am.get(log['fase'].split()[-1], '')
                        if not id_fase:
                            if not error_am_dip:
                                error_msg = '\tFase AM MMFG non mappata per l\'anno {0}.'.format(self.year)
                                self.report_error(error_msg)
                                error_am_dip = True
                            continue
                    else:
                        select = ("""
                            SELECT id_fase
                            FROM fase
                            WHERE descrizione = '{0}'
                            AND id_commessa = {1}
                        """.format(log['descrizione'], id_commessa))
                        id_fase = self.get_id(select)

                    if id_fase == -1:
                        next_val = self.get_id('SELECT seq_fase.nextval from dual')
                        insert = ("""
                            INSERT INTO fase (id_stato_fase, descrizione, id_tipo_fase, id_commessa, id_fase)
                            VALUES (1, '{0}', 11, {1}, {2})
                        """.format(log['descrizione'], id_commessa, next_val))
                        id_fase = self.add_id(insert, select)
                        if id_fase == -1:
                            error_msg = '\tCreazione fase {0} per commessa {1} di applicazione {2} non riuscita.'.format(log["fase"], commessa, log['applicazione'])
                            self.report_error(error_msg)
                            continue
                        self.output_logger('Fase {0} creata'.format(id_fase))
                    else:
                        self.output_logger('Fase trovata ---> {0} con codice {1}'.format(id_fase, log["fase"]))
                    # Controllo che la fase sia visibile al dipendente
                    self.check_status("fase", id_fase, id_dipendente)
                    # Aggiungo la fase tra quelle del dipendente
                    if id_fase not in self.fasi_dipendente:
                        self.fasi_dipendente.append(id_fase)

                    # Controllo l'esistenza del day tracking. In caso di mancanza vine creato un nuovo record e in caso
                    # di errore viene avviata la segnalazione.
                    select = ("""
                        SELECT id_day_tracking
                        FROM day_tracking
                        WHERE giorno=TO_DATE('{0} 00:00:00', 'YYYY-MM-DD HH24-MI-SS')
                        AND id_dipendente = {1}
                    """.format(log_date, id_dipendente))
                    id_day_tracking = self.get_id(select)

                    if id_day_tracking == -1:
                        next_val = self.get_id('SELECT seq_day_tracking.nextval from dual')
                        insert = ("""
                            INSERT INTO day_tracking (id_day_tracking, id_stato_validazione, id_dipendente, ore_assenza, giorno, notte_fuori, data_salvataggio, mensa_cliente, convenzione_pasto)
                            VALUES ({0}, 1, {1}, 0, TO_DATE('{2} 00:00:00', 'YYYY-MM-DD HH24-MI-SS'), 'N', TO_DATE('{3} 00:00:00', 'YYYY-MM-DD HH24-MI-SS'), 'N', 'N')
                            """.format(next_val, id_dipendente, log_date, date.today()))
                        id_day_tracking = self.add_id(insert, select)
                        if id_day_tracking == -1:
                            error_msg = '\tCreazione day_tracking per giorno {0} 00:00:00 associata a id_dipendente {1} non riuscita.'.format(log_date, id_dipendente)
                            self.report_error(error_msg)
                            continue
                        self.output_logger('ID_DAY_TRACKING {0} creato'.format(id_day_tracking))
                    else:
                        self.output_logger('ID_DAY_TRACKING trovato ---> {0}'.format(id_day_tracking))

                    # Richiamo la funzione che gestisce la creazione dei ticket
                    self.ticketing(log_date, id_day_tracking, id_fase, log)

            # Se rimangono delle fasi associate ad un dipendente elimino i ticket riguardanti il mese corrente
            if self.fasi_recuperate:
                self.delete_tickets_by_fasi(id_dipendente)

    def informa_project_lead(self, single_dip_error, issues, dipendente):
        """
            Informa il project lead delle ore loggate da un dipendente su story che presentano subtask.
        """
        affected_project = set()
        for issue in issues:
            affected_project.add(issue.split('-')[0])

        for project in affected_project:
            project_lead = self.jira_client.project(project).lead.name + '@otconsulting.com'
            issue_segnalate = [issue for issue in issues if issue.startswith(project)]
            error = '{0} ha inserito delle ore nelle seguenti story che presentano dei subtask.\n{1}: ({2})\n'.format(dipendente, project, ', '.join(issue_segnalate))
            single_dip_error[project_lead.lower()] = single_dip_error.setdefault(project_lead.lower(), '') + '\n' + error

    def send_mail(self, s, to_email, error_msg):
        """
            Metodo per l'invio della email con gli errori trovati nei Timesheet.
        """
        # Prepara l'intestazione e il corpo della mail
        msg = MIMEText(error_msg)
        msg['Subject'] = "[JIRA Job] Errori job Timesheet"
        msg['From'] = self.from_email
        msg['To'] = to_email

        s.sendmail(self.from_email, [to_email], msg.as_string())

    def format_mail(self):
        """
            Metodo che prepara i messaggi di errore da inviare all'amministratore ed ai singoli dipendenti.
        """
        single_dip_error = {}
        password = self.email_password
        to_email = self.to_email

        # Connessione al server smtp ed invio della mail
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(self.from_email, password)

        # Scorro gli errori per formare un messaggio comprensibile
        error_msg = ''
        for key, value in self.log_error.iteritems():
            if '@otconsulting.com' not in key.lower() or '@cognitive.com.mt' not in key.lower():
                # Errori non legati ad un dipendente
                error_msg = '- {0}: {1}\n\n{2}'.format(value, key, error_msg)
                continue
            else:
                # Errori legati ad un dipendete
                error_msg += '- {0}: ({1})\n'.format(key.lower().replace('@otconsulting.com', '').replace('.', ' ').title(), key)
                for error in value:
                    error_msg += '{0}\n'.format(error)
                error_msg += '\n'

        if self.wrong_log:
            error_msg += 'Log inseriti in story con subtask.\n'
            for email_dip in self.wrong_log:
                formatted_email_dip = email_dip.lower().replace('@otconsulting.com', '').replace('@cognitive.com.mt', '').replace('.', ' ').title()
                error = '\t{0}: ({1})\n\n'.format(formatted_email_dip, ', '.join(self.wrong_log[email_dip]))
                error_msg += error
                error = 'Log inseriti in story con subtask.\n' + error
                single_dip_error[email_dip.lower()] = error
                self.informa_project_lead(single_dip_error, self.wrong_log[email_dip], formatted_email_dip)

        if self.no_epic:
            error_msg += 'Issue senza epic in cui sono presenti loggate.\n'
            for project, issues in self.no_epic.iteritems():
                project_lead = self.jira_client.project(project).lead.name + '@otconsulting.com'
                error = '\t{0}: ({1})\n\n'.format(project, ', '.join(issues))
                error_msg += error
                error = 'Issue senza epic in cui sono presenti loggate.\n' + error
                single_dip_error[project_lead.lower()] = single_dip_error.setdefault(project_lead.lower(), '') + '\n' + error

        for email_dip in single_dip_error:
            self.send_mail(s, email_dip, single_dip_error[email_dip])

        self.send_mail(s, to_email, error_msg)

        s.quit()

    def quadratura_ore(self):
        """
            Riassegna le ore decimali trovate durante la fase di raccolta dei log inseriti dai dipendenti su JIRA.
            Le ore vengono troncate per difetto!!!
        """
        for email_dip, date_list in self.ore_decimali.iteritems():
            for log_date, ore_decimali in self.ore_decimali[email_dip].iteritems():
                ore_da_assegnare = int(ore_decimali)
                if ore_da_assegnare > 0:
                    self.work_log[email_dip][log_date][0]['log_time'] = ore_da_assegnare + self.work_log[email_dip][log_date][0].get('log_time', 0)

    def skip_log_date(self, log_date):
        """
            Controlla che la data di log sia all'interno del range di valutazione e che non sia una Domenica
        """
        # La data del log deve essere all'interno del range di valutazione
        if not re.search('^' + str(self.year) + '-0*' + str(self.month), log_date):
            return True

        # Skippo le loggate effettuate sulla Domenica.
        # Il replace su log_date e' necessario in quando strptime richiede solo due cifre per l'anno.
        if datetime.strptime(log_date.replace('20', '', 1), '%y-%m-%d').strftime('%A') == 'Sunday':
            return True

        return False

    def build_dict_worklog(self, log_info, log_author, log_date):
        """
            Date le info recuperate da un worklog aggiorna il dizionario contenente tutte le loggate
        """
        day_range = calendar.monthrange(self.year, self.month)
        if log_author not in self.work_log:
            # Manca ancora il dipendente:
            # - Aggiunta del dipendente al dict
            # - Aggiunta di una data per ciascun giorno del mese
            self.work_log[log_author] = {}
            for day_num in range(1, day_range[1] + 1):
                day = unicode(date(self.year, self.month, day_num))
                self.work_log[log_author][day] = []

        # Siccome deve esserci un ticket al giorno per ogni fase, controllo se nel primo giorno del mese e' presente
        # una loggata con fase uguale alle info correnti. In caso contrario, per ogni giorno del mese, aggiungo le
        # 'log_info' con 0 ore lavorate.
        found = False
        day = unicode(date(self.year, self.month, 01))
        for work_logged in self.work_log[log_author][day]:
            if work_logged['descrizione'] == log_info['descrizione']:
                found = True
                break

        if not found:
            for day_num in range(1, day_range[1] + 1):
                log_info_clean = log_info.copy()
                log_info_clean['log_time'] = 0
                day = unicode(date(self.year, self.month, day_num))
                self.work_log[log_author][day].append(log_info_clean)

        # Cerco se le info sono gia' state loggate in precedenza per il fatto che fase e commessa vengono
        # mappate 1:1.
        # Su JIRA si loggano le ore nelle sottoissue delle Epic ma il tutto viene riportato nella Epic stessa
        # che va a definire questa mappatura.
        # Viene inserito un nuovo elemento nella lista se anche un solo campo tra i dict comparati risulta
        # diverso, ad eccezione di 'log_time'. Se si trova una corrispondenza si modifica il vecchio log
        # aggiungendo le ore.
        is_equal = False
        for old_log in self.work_log[log_author][log_date]:
            for key, value in log_info.iteritems():
                if key == 'log_time':
                    continue
                elif old_log[key] == value:
                    is_equal = True
                else:
                    is_equal = False
                    break

            if is_equal:
                # Somma i log_time
                old_log['log_time'] += log_info['log_time']
                break
        if not is_equal:
            self.work_log[log_author][log_date].append(log_info)

    def grep_am_worklog(self):
        """
            Recupera la ore loggate per l'AM di MMFG
        """
        project = self.jira_client.project('OTBMS')

        worklogs = self.jira_client.worklogs(self.issue_am)
        for log in worklogs:
            # Recupero la data a cui si riferisce il log
            log_date = re.search('^[\d]+-[\d]+-[\d]+T', log.started).group(0).replace('T', '')
            if self.skip_log_date(log_date):
                continue

            # Recupero l'autore del worklog skippando sempre il domain admin
            log_author = log.author.emailAddress
            if log_author == self.from_email or 'cognitive.com.mt' in log_author:
                continue

            # Recupero le ore loggate esprimendole in ore e troncandole per difetto
            log_time = int(log.timeSpentSeconds / 3600)

            # Raccolta informazioni
            log_info = {
                'cliente': project.raw['projectCategory']['name'],
                'applicazione': project.key,
                'commessa': 'MMFG_AM_{}'.format(self.year),
                'commessa_default': '',
                'fase': 'AM MMFG {}'.format(self.year),
                'descrizione': 'Attivita adeguativa correttiva',
                'log_time': log_time,
            }
            self.build_dict_worklog(log_info, log_author, log_date)

    def grep_worklog(self, project, epic_issue, worklogs):
        """
            Recupera i log recuperati da una specifica issue
        """
        for log in worklogs:
            # Recupero la data a cui si riferisce il log
            log_date = re.search('^[\d]+-[\d]+-[\d]+T', log.started).group(0).replace('T', '')
            if self.skip_log_date(log_date):
                continue

            # Recupero l'autore del worklog skippando sempre il domain admin
            log_author = log.author.emailAddress
            if log_author == self.from_email or 'cognitive.com.mt' in log_author:
                continue

            # Recupero la parte decimale delle ore lavorate e la sommo a quelle precedentemente accumulate dal
            # dipendente.
            log_time = int(log.timeSpentSeconds / 3600)
            if log_author not in self.ore_decimali:
                self.ore_decimali[log_author] = {}
            self.ore_decimali[log_author][log_date] = float(self.ore_decimali[log_author].get(log_date, 0)) + float(log.timeSpentSeconds)/3600 - log_time

            # Raccolta informazioni
            log_info = {
                'cliente': project.raw['projectCategory']['name'],
                'applicazione': project.key,
                'commessa': epic_issue.raw['fields'].get('customfield_10037', ''),
                'commessa_default': 'segnaposto ' + project.key + '_' + epic_issue.id,
                'fase': epic_issue.fields.summary.split('-')[0],
                'descrizione': unicodedata.normalize('NFD', epic_issue.fields.summary.replace("'", "''")).encode('ascii', 'ignore'),
                'log_time': log_time,
            }
            self.build_dict_worklog(log_info, log_author, log_date)

    def issue_has_to_be_reported(self, issue, issue_worklogs):
        """
            Per evitare che vengano riportate issue senza epic che non presentano loggate nel mese di valutazione si
            esegue un ciclo di controllo sulla data dei worklogs inseriti in essa o nei subtask associati.
        """
        # Verifico i log della issue
        for log in issue_worklogs:
            log_date = re.search('^[\d]+-[\d]+-[\d]+T', log.started).group(0).replace('T', '')
            if self.skip_log_date(log_date):
                return True

        # Verifico i log dei subtask associati
        for subtask in issue.fields.subtasks:
            worklogs = self.jira_client.worklogs(subtask.key)
            for log in worklogs:
                log_date = re.search('^[\d]+-[\d]+-[\d]+T', log.started).group(0).replace('T', '')
                if self.skip_log_date(log_date):
                    return True

        return False

    def work_with_issue(self, project, issue):
        """
            Data una issue di un progetto ricerco il valore contenuto nel 'customfield_100005' in cui e' contenuto il
            codice (Jira) della Epic a cui e' linkata. Se non e' presente si skippa la issue.
            In seguito si recuperano i worklogs.
        """
        # Skippo la issue riguardante l'AM MMFG perche' viene elaborata a parte
        if issue.key == self.issue_am:
            return

        # Recupero i worklog della issue
        worklogs = self.jira_client.worklogs(issue.key)

        epic_issue_id = issue.raw['fields'].get('customfield_10005', '')
        try:
            epic_issue = self.jira_client.issue(epic_issue_id)
        except Exception as ex:
            if self.issue_has_to_be_reported(issue, worklogs):
                self.no_epic.setdefault(project.key, set()).add(issue.key)
            return

        if issue.fields.subtasks:
            # Se ci sono dei log nella story li scorro per segnalare l'errore agli utenti che li hanno inseriti
            for log in worklogs:
                # Recupero la data a cui si riferisce il log
                log_date = re.search('^[\d]+-[\d]+-[\d]+T', log.started).group(0).replace('T', '')
                if self.skip_log_date(log_date):
                    continue

                # Recupero l'autore del worklog skippando domain.adm
                log_author = log.author.emailAddress
                if log_author == self.from_email:
                    continue

                self.wrong_log.setdefault(log_author, set()).add(issue.key)

            # Per ogni subtask recupero i log e li elaboro
            for subtask in issue.fields.subtasks:
                worklogs = self.jira_client.worklogs(subtask.key)
                self.grep_worklog(project, epic_issue, worklogs)
        else:
            # Non ci sono subtask quindi prendo elaboro i log della story
            self.grep_worklog(project, epic_issue, worklogs)

    def validate_ts(self):
        """
            Recupero da JIRA i progetti creati e per ciascuno estraggo le issue di tipo NON 'Epic'
        """
        if date.today().day <= 10:
            if date.today().month == 1:
                self.month = 12
                self.year = date.today().year - 1
            else:
                self.month = date.today().month - 1
                self.year = date.today().year
        else:
            self.month = date.today().month
            self.year = date.today().year
        self.output_logger("Periodo di retrieve delle issue:\nAnno {0}\nMese {1}".format(self.year, calendar.month_name[self.month]))

        projects = self.jira_client.projects()
        for project in projects:
            if project.key not in self.applicazioni:
                self.output_logger("\nProgetto {0} skippato".format(project.key))
                continue

            # E' stato fatto un cablone dal 1 Gennaio perche' non venivano recuperati i subtask linkati ad epic
            # non aggiornate nel mese di valutazione
            issues = self.jira_client.search_issues("""
                project = {0}
                and type not in (Epic, Sub-task, "Technical task.", Story, Task, Project)
                and updated >= 2016-06-01
                """.format(project.key),
                maxResults=5000)
            self.output_logger("\nProgetto {0} del cliente {1} contiene {2} issues".format(project.key, project.raw['projectCategory']['name'], len(issues)))
            for issue in issues:
                self.work_with_issue(project, issue)

        # Eseguo la quadratura ore
        self.quadratura_ore()

        # Aggiungo le ore relative all'AM MMFG
        self.grep_am_worklog()

        # Trascrive le ore trovate su Jira nel Timesheet OT
        self.write_log()

        # In caso siano stati riscontrati errori li invia per mail all'amministratore
        if self.log_error or self.wrong_log or self.no_epic:
            self.format_mail()

if __name__ == '__main__':
    jira = JiraTsReporter()
    #jira.clean_database([])
    jira.validate_ts()
    jira.close()
