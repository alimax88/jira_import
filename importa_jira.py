#!/usr/bin/env python
# -*- coding: utf-8 -*-

import jira
import xlwt
import requests
import re
import argparse
import sys

from itertools import groupby
from operator import itemgetter
import config as jira_config
import pprint
import os

import calendar
from datetime import date, datetime

# per lanciarlo da MaxMara
# export HTTP_PROXY=http://proxy.mmfg.it:8080
# prima di lanciarlo
logger_actived = True

class JiraExtractor:
	def __init__(self, data = {}):
		# Data for connection JIRA
		jira_user = str(input('Inserire user jira tra virgolette\n'))
		jira_psw = str(input('Inserire pw jira tra virgolette\n'))
		jira_options = {
			'server': str(input('Inserire server jira tra virgolette\n')),
			'verify': "False"
		}
		self.jira_client = jira.JIRA(options=jira_options, basic_auth=(jira_user, jira_psw))
		self.progetti_da_esaminare = data.get('progetto', jira_config.progetti_da_esaminare)
		if data.get('date_from'):
			self.date_from = data['date_from']
		if data.get('date_to'):
			self.date_to = data['date_to']

	def output_logger(self, string):
		if logger_actived:
			print string

	def get_worlogs_with_tempo(self, issue_key):
		# Gets all worklogs for a given period, returns data on specified format
		# serve un security token!!!
		url = 'http://jira.otconsulting.com/plugins/servlet/tempo-getWorklog/?'
		params = {'dateFrom': 	self.date_from,
				  'dateTo': 	self.date_to,
				  'format': 	"testData",
				  'addBillingInfo': True,
				  'addWorklogDetails': True,
				  'issueKey':	issue_key,
				  'tempoApiToken': 'provaunoacaso'}
		r = requests.get(url, params=params)
		print r
		worklog_data = r.json()
		pprint.pprint(worklog_data)

	def get_all_issue_by_projects(self):
		projects = self.jira_client.projects()
		list_issues = []
		for project in projects:
			if project.key in self.progetti_da_esaminare:
				print project
				# prendo solo le production perche' si logga su queste
				# o altrimenti sui sottotask
				issues = self.jira_client.search_issues("""
							  		project = {0}
							  and 	type in (Production)
							  AND 	worklogDate >= "{1}"
							  AND 	worklogDate <= "{2}"
							  and 	status != resolved
							  """.format(project.key, self.date_from, self.date_to), maxResults=5000)
				print issues
				list_issues.extend(issues)

		# cerco i sottotask
		for issue in issues:
			for subtask in issue.raw['fields']['subtasks']:
				sub_issue = self.jira_client.issue(subtask['id'])
				list_issues.append(sub_issue)

		return list_issues

	@staticmethod
	def calculate_over_time(time_spent, date):
		# calculate ove time
		my_date = datetime.strptime(date, '%Y-%m-%d')

		day_of_week = calendar.day_name[my_date.weekday()]
		over_time = 0
		if date in jira_config.ECB_HOLIDAYS or day_of_week.lower() not in jira_config.WORKING_DAYS:
			tmp = time_spent
			over_time = tmp
			time_spent = 0

		else:
			if time_spent > 8:
				over_time = time_spent - 8
				time_spent = 8

		return time_spent, over_time


	def get_worked_log_by_issue_from_date(self, issue_obj):
		list_all_work_log_issue = []
		worklogs = self.jira_client.worklogs(issue_obj)


		for log in worklogs:
			log_author = log.author.emailAddress
			data_log = re.search('^[\d]+-[\d]+-[\d]+T', log.started).group(0).replace('T', '')
			date_from = self.date_from.replace('/','-')
			date_to = self.date_to.replace('/', '-')
			_cache_date = {}
			if data_log >= date_from and data_log <= date_to:
				#if log_author == 'alice.mazzoli@otconsulting.com':
				#	pprint.pprint(log)
				# prendo i secondi e li trasformo in ore
				# ho provato la via delle ore ma diventa un casino con i minuti
				time_spent = log.timeSpentSeconds / 3600

				if '30m' in log.timeSpent :
					time_spent = time_spent + 0.5

				diz_work_log = {}
				diz_work_log['issue'] = issue_obj
				diz_work_log['date'] = data_log
				diz_work_log['author'] = log_author
				diz_work_log['time_spent'] = time_spent
				list_all_work_log_issue.append(diz_work_log)


		return list_all_work_log_issue



def estrai_ore(diz):
	jira_obj = JiraExtractor(diz)

	lista_issues = jira_obj.get_all_issue_by_projects()
	diz_totale_issue_time = {}
	lista_worklog_on_issues = []
	for issue in lista_issues:
		diz_totale_issue_time[issue] = {}
		lista_worklog_on_issues.extend(jira_obj.get_worked_log_by_issue_from_date(issue))

	# raggruppo per autore e data, calcolando gli straordinari per ogni giorno
	# che poi spostero' sulla prima issue che capita perche'  non ho informazioni
	# precise
	grouper = itemgetter("author", "date")
	lista_worklog_on_issues = sorted(lista_worklog_on_issues, key=grouper)
	worked_for_issue = 0
	lista_diz = []
	for key, g in groupby(lista_worklog_on_issues, grouper):
		grouped = list(g)
		all_time_spent = sum([i['time_spent'] for i in grouped])
		time_spent, over_time = JiraExtractor.calculate_over_time(all_time_spent,key[1])

		# cerco di distribuire lo straordinario sulla issue giusta
		for work_issue_diz in grouped:
			diz = {}
			diz['author'] = key[0]
			diz['date'] = key[1]
			diz['issue'] = work_issue_diz['issue']
			# di solito gli straordinari vanno segnati su una issue tutti insieme
			# e si segnano solo le ore di straordinario, non 9 ore ad esempio
			# perche' vanno dichiarati di straordinario.
			if work_issue_diz['time_spent'] == over_time:
				work_issue_diz['time_spent'] = 0
				work_issue_diz['over_time'] = over_time
			else:
				# time_spent rimane uguale, azzero l'over time
				work_issue_diz['over_time'] = 0
			# print "in data {date} , {author} ha loggato {time_spent} h ord e {over_time} h straord".format(** work_issue_diz)
			lista_diz.append(work_issue_diz)

	# adesso metto in chiave le issue e per valore i valori
	grouper = itemgetter("issue")
	lista_diz = sorted(lista_diz, key=grouper)
	for issue_obj, k in groupby(lista_diz, grouper):
		rec_for_issues = list(k)
		issue = issue_obj
		grouper_author = itemgetter("author")
		rec_for_issues = sorted(rec_for_issues, key=grouper_author)
		worked_for_issue = extra_for_issue = 0
		# print 'JIRAMMFG \t\t Author \t\t\t time spent \t straordinario'
		for author, g in groupby(rec_for_issues, grouper_author):
			worked_by_author = list(g)
			worked_total = sum(i['time_spent'] for i in worked_by_author)
			extra_total = sum(i['over_time'] for i in worked_by_author)
			worked_for_issue += worked_total
			extra_for_issue += extra_total

			diz_totale_issue_time[issue][author] = {}
			diz_totale_issue_time[issue][author]['worked_total'] = worked_total
			diz_totale_issue_time[issue][author]['extra_total'] = extra_total

		# print '{0} \t {1} \t {2} \t\t\t {3}'.format(issue.raw['fields']['customfield_10040'], author, worked_total, extra_total)

		# print 'Totale conteggiato {0} di cui {1} ore di straoridnario '.format(worked_for_issue, extra_for_issue)

	return diz_totale_issue_time

def create_excel(diz_worklog, mese, anno):
	# in chiave mi aspeetto la issue di tipo production
	# valore = dizionario

	book = xlwt.Workbook()
	sheet_tot_commessa = book.add_sheet("Totali per commessa", cell_overwrite_ok=True)
	sheet_team = book.add_sheet("Totali per persona", cell_overwrite_ok=True)

	style_bold = xlwt.easyxf('font: bold 1')
	style = xlwt.easyxf('font: name Calibri')

	# titolo
	riga = 0
	num_max_colonne = 10

	sheet_tot_commessa.write(riga, 1, 'Commessa OT', style_bold)
	sheet_tot_commessa.write(riga, 2, 'Epic', style_bold)
	sheet_tot_commessa.write(riga, 3, 'Codice Jira OT', style_bold)
	sheet_tot_commessa.write(riga, 4, 'Codice Jira MM', style_bold)
	sheet_tot_commessa.write(riga, 5, 'Descrizione', style_bold)
	sheet_tot_commessa.write(riga, 6, 'GG Commessa', style_bold)
	sheet_tot_commessa.write(riga, 7, 'GG Task', style_bold)
	sheet_tot_commessa.write(riga, 8, 'SAL', style_bold)
	sheet_tot_commessa.write(riga, 9, 'Residui', style_bold)
	sheet_tot_commessa.write(riga, 10, 'Tot ord mese (h)', style_bold)
	sheet_tot_commessa.write(riga, 11, 'Tot extra mese (h)', style_bold)
	sheet_tot_commessa.write(riga, 12, 'Tot gg da segnare (PROPOSTA)', style_bold)

	riga += 1

	diz_for_author = {}
	# '['customfield_10040']
	diz_for_author = {}
	# pprint.pprint(diz_worklog)
	for issue in diz_worklog:
		tot_issue = tot_over = 0

		for author in diz_worklog[issue]:
			tot_issue += float(diz_worklog[issue][author]['worked_total'])
			with_markup = float(diz_worklog[issue][author]['extra_total']) * jira_config.MARKUP_EXTRA
			tot_over += with_markup
			tot_dd = tot_over + tot_issue
			tot_dd = tot_dd / 8

			if author not in diz_for_author:
				diz_issue = diz_worklog[issue][author]
				diz_issue['issue'] = issue
				diz_for_author[author] = [diz_issue]
			else:
				diz_issue = diz_worklog[issue][author]
				diz_issue['issue'] = issue
				diz_for_author[author].append(diz_issue)

		# pprint.pprint(issue.raw)
		sheet_tot_commessa.write(riga, 1, issue.fields.customfield_10037, style) # commessa OT
		sheet_tot_commessa.write(riga, 2, issue.fields.customfield_10005, style) # codice epic
		sheet_tot_commessa.write(riga, 3, issue.key , style) # codice jira OT
		sheet_tot_commessa.write(riga, 4, issue.fields.customfield_10040, style) # codice jira MM
		sheet_tot_commessa.write(riga, 5, issue.fields.summary, style) # descrizione
		sheet_tot_commessa.write(riga, 6, '', style) #GG commessa
		sheet_tot_commessa.write(riga, 7, '', style) #GG Task
		sheet_tot_commessa.write(riga, 8, '', style) #SAL
		sheet_tot_commessa.write(riga, 9, '', style) #Residui
		sheet_tot_commessa.write(riga, 10, tot_issue , style) # tot ore mese
		sheet_tot_commessa.write(riga, 11, tot_over, style) # tot over mese
		sheet_tot_commessa.write(riga, 12, tot_dd, style) # tot gg da segnare

		riga += 1
	# pprint.pprint(diz_for_author)
	# adesso il foglio per persona
	style_bold = xlwt.easyxf('pattern: pattern solid, fore_colour light_blue;'
                              'font: colour white, bold True;')
	riga = 0
	for author in diz_for_author:
		sheet_team.write(riga, 0, author, style_bold)
		sheet_team.write(riga, 1, 'Commessa OT', style_bold)
		sheet_team.write(riga, 2, 'Epic', style_bold)
		sheet_team.write(riga, 3, 'Codice Jira OT', style_bold)
		sheet_team.write(riga, 4, 'Codice Jira MM', style_bold)
		sheet_team.write(riga, 5, 'Descrizione', style_bold)
		sheet_team.write(riga, 6, 'Tot ord mese (h)', style_bold)
		sheet_team.write(riga, 7, 'Tot extra mese (h)', style_bold)
		sheet_team.write(riga, 8, 'Tot extra mese + MK (h)', style_bold)
		sheet_team.write(riga, 9, 'Tot mese (h)', style_bold)
		sheet_team.write(riga, 10, 'Tot dd mese', style_bold)
		riga += 1

		for issue_diz in diz_for_author[author]:
			extra_with_markup = issue_diz['extra_total'] * jira_config.MARKUP_EXTRA
			tot_dd = issue_diz['worked_total'] + extra_with_markup
			tot_dd = tot_dd / 8

			issue = issue_diz['issue']
			sheet_team.write(riga, 1, issue.fields.customfield_10037, style) # commessa OT
			sheet_team.write(riga, 2, issue.fields.customfield_10005,style)  # codice epic
			sheet_team.write(riga, 3, issue.key,style)  # codice jira OT
			sheet_team.write(riga, 4, issue.fields.customfield_10040,style)  # codice jira MM
			sheet_team.write(riga, 5, issue.fields.summary,style)  # descrizione
			sheet_team.write(riga, 6, issue_diz['worked_total'], style)
			sheet_team.write(riga, 7, issue_diz['extra_total'] , style) # over normal
			sheet_team.write(riga, 8, extra_with_markup , style)  # over markup
			sheet_team.write(riga, 9, issue_diz['worked_total'] + extra_with_markup , style)  # tot ord and overtime with markup
			sheet_team.write(riga, 10, tot_dd , style)
			riga += 1

	# saving file
	path_to_save = jira_config.PATH_FILE + "TIMESHEET_{}".format(anno)
	if not os.path.isdir(path_to_save):
		os.mkdir(path_to_save)
	book.save(path_to_save + "/{}_{}.xls".format(mese, anno))


def create_text_email_ced(diz_worklog, anno, mese, date_from):
	lista_dati_issue = []
	for issue in diz_worklog:
		tot_issue = tot_over = 0
		tot_dd = 0
		for author in diz_worklog[issue]:
			tot_issue += float(diz_worklog[issue][author]['worked_total'])
			with_markup = float(diz_worklog[issue][author][
									'extra_total']) * jira_config.MARKUP_EXTRA
			tot_over += with_markup
			tot_dd = tot_over + tot_issue
			tot_dd = tot_dd / 8
		codice_mmfg = issue.fields.customfield_10040  if issue.fields.customfield_10040  else '<emtpy>'
		lista_dati_issue.append("- " + codice_mmfg + " " + issue.fields.summary + "\t" + str(tot_dd) + " gg")

	stringa_issue = '\n'.join(lista_dati_issue)

	contenuto = ''' Ciao ,
		ecco l'avanzamento lavori al {0}:
		Il SAL (con residuo giornate da offerta) delle commesse è il seguente:
		{1}
		'''.format(date_from, stringa_issue)

	# saving file
	path_to_save = jira_config.PATH_FILE + "TIMESHEET_{}".format(anno)
	if not os.path.isdir(path_to_save):
		os.mkdir(path_to_save)
	open(path_to_save + '/text_email_{}_{}.txt'.format(anno, mese), 'w').write(contenuto)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description='''
		#################### ####################
		Script per estrarre le ore del team OTMM
		#################### ####################
			''')
	parser.add_argument('--progetto',
						dest='progetto',
						help='Progetto da esaminare, di default, se non specificato \
							  in ingresso prende la lista dei progetti nel config (per ora solo OTMM) ',
						required=False
						)

	parser.add_argument('--date_from',
						dest='date_from',
						help='formato ex: 2017/01/01',
						required=True
						)

	parser.add_argument('--date_to',
						dest='date_to',
						help='formato ex: 2017/01/31',
						required=True
						)

	parser.add_argument('--mese',
						dest='mese',
						help='mese di cui estrarre le ore formato gennaio/febbraio...',
						required=True
						)

	parser.add_argument('--anno',
						dest='anno',
						help='anno solare ex 2017',
						required=True
						)

	parser.add_argument('--operazione',
						dest='operazione',
						help='1) excel: crea l\' excel con le ore , \n2)lettera: crea solo la lettera da inviare al ced, \n3)all: crea entrambi',
						required=True
						)

	namespace = parser.parse_args()
	if namespace.operazione not in ('all', 'excel', 'lettera'):
		print 'Operazione non ammessa'
		sys.exit(1)

	mese = namespace.mese
	anno = namespace.anno
	diz = {'date_from': namespace.date_from,
		   'date_to': namespace.date_to }

	diz_total = estrai_ore(diz)
	if namespace.operazione in ('excel', 'all'):
		create_excel(diz_total, mese, anno)
	if namespace.operazione in ('lettera', 'all'):
		create_text_email_ced(diz_total, anno, mese, diz['date_from'])
