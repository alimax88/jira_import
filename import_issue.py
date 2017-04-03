''' Modulo per recuperare epis/task e sub task
quindi capire cosa ci sia di aperto ancora o chiuso...
'''

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
import importa_jira

import calendar
from datetime import date, datetime

logger_actived = True

def get_all_issue():
	obj_extractor = importa_jira.JiraExtractor()


	projects = obj_extractor.jira_client.projects()
	list_issues = []
	for project in projects:
		if project.key in obj_extractor.progetti_da_esaminare:
			print project
			# prendo solo le production perche' si logga su queste
			# o altrimenti sui sottotask
			issues = obj_extractor.jira_client.search_issues("""
									project = {0}
							  AND 	type != epic
							  AND 	status in (open, "In Progress")
							  """.format(project.key), maxResults=5000)
			list_issues.extend(issues)


	diz_issues = {}
	diz_sottotask = {}
	for item in list_issues:

		key_task = item.raw['key']
		code_fg = item.raw['fields']['customfield_10040']
		commessa_ot = item.raw['fields']['customfield_10037']
		descrizione = item.raw['fields']['summary']
		type = item.raw['fields']['issuetype']['name']
		is_subtask = item.raw['fields']['issuetype']['subtask']
		if is_subtask:
			# ne tengo traccia e poi dopo li mettero' nel dizionario padre
			key_issue_padre = item.raw['fields']['parent']['key']
			if key_issue_padre not in diz_sottotask:
				diz_sottotask[key_issue_padre] = []
			diz_sottotask[key_issue_padre].append({ 'code_fg': code_fg,
													'commessa_ot' : commessa_ot,
													'descrizione': descrizione,
													'key_issue':key_task })
		else:
			epic_key = item.raw['fields']['customfield_10005']
			if epic_key not in diz_issues:
				epic_issue = obj_extractor.jira_client.issue(epic_key)
				diz_issues[epic_key] = { 'code_fg':  epic_issue.raw['fields']['customfield_10040'],
											   'commessa_ot' : epic_issue.raw['fields']['customfield_10037'],
											   'descrizione': epic_issue.raw['fields']['summary'],
											   'task': {key_task : { 	'code_fg': code_fg,
											  					'commessa_ot' : commessa_ot,
											   					'descrizione': descrizione,
											   					'sotto_task': [] }}}

			else:
				if key_task not in diz_issues[epic_key]['task']:
					diz_issues[epic_key]['task'][key_task] = { 	'code_fg': code_fg,
											  					'commessa_ot' : commessa_ot,
											   					'descrizione': descrizione,
											   					'sotto_task': [] }

	# arrivata qui ho costruito il dizionario con epic e task, un altro dizionario con solo i sottotask
	# li mergio mettendo i sottotask
	for key_epic in diz_issues:
		for key_task in diz_issues[key_epic]['task']:
			if key_task in diz_sottotask:
				diz_issues[key_epic]['task'][key_task]['sotto_task'] = diz_sottotask[key_task]
	return diz_issues

def create_excel(diz_issues):
	# in chiave mi aspeetto la issue di tipo production
	# valore = dizionario

	book = xlwt.Workbook()
	sheet_task_aperti = book.add_sheet("Task aperti Jira OT",cell_overwrite_ok=True)

	style_bold_epic = xlwt.easyxf(
		'pattern: pattern solid, fore_colour light_blue;'
		'font: colour white, bold True;')
	style_bold_task = xlwt.easyxf(
		'pattern: pattern solid, fore_colour gray40;'
		'font: colour white, bold True;')
	style_bold_subtask = xlwt.easyxf(
		'pattern: pattern solid, fore_colour gray25;'
		'font: colour white, bold True;')
	style = xlwt.easyxf('font: name Calibri; ')


	# titolo
	riga = 0
	num_max_colonne = 10



	for epic in diz_issues:
		livello = 0
		sheet_task_aperti.write(riga, 1+ livello, 'Commessa riferimento', style_bold_epic)
		sheet_task_aperti.write(riga, 2+ livello, 'Codice JIRA OT', style_bold_epic)
		sheet_task_aperti.write(riga, 3+ livello, 'Codice JIRA MM', style_bold_epic)
		sheet_task_aperti.write(riga, 4+ livello, 'Descrizione', style_bold_epic)
		riga += 1

		sheet_task_aperti.write(riga, 1+livello, 	diz_issues[epic]['commessa_ot'], style)
		sheet_task_aperti.write(riga, 2+livello, 	epic, style)
		sheet_task_aperti.write(riga, 3+livello,  	diz_issues[epic]['code_fg'], style)
		sheet_task_aperti.write(riga, 4+livello,  	diz_issues[epic]['descrizione'], style)
		riga += 1

		livello = 1
		sheet_task_aperti.write(riga, 1 + livello, 'Commessa riferimento',
								style_bold_task)
		sheet_task_aperti.write(riga, 2 + livello, 'Codice JIRA OT',
								style_bold_task)
		sheet_task_aperti.write(riga, 3 + livello, 'Codice JIRA MM',
								style_bold_task)
		sheet_task_aperti.write(riga, 4 + livello, 'Descrizione',
								style_bold_task)
		riga += 1
		for task in diz_issues[epic]['task']:


			sheet_task_aperti.write(riga, 1 + livello, diz_issues[epic]['task'][task]['commessa_ot'],style)
			sheet_task_aperti.write(riga, 2 + livello, task, style)
			sheet_task_aperti.write(riga, 3 + livello, diz_issues[epic]['task'][task]['code_fg'], style)
			sheet_task_aperti.write(riga, 4 + livello, diz_issues[epic]['task'][task]['descrizione'],style)
			riga += 1
			if diz_issues[epic]['task'][task]['sotto_task']:
				livello_sottotask = 5
				sheet_task_aperti.write(riga, 1+ livello_sottotask, 'Commessa riferimento',style_bold_subtask)
				sheet_task_aperti.write(riga, 2+ livello_sottotask, 'Codice JIRA OT', style_bold_subtask)
				sheet_task_aperti.write(riga, 3+ livello_sottotask, 'Codice JIRA MM', style_bold_subtask)
				sheet_task_aperti.write(riga, 4+ livello_sottotask, 'Descrizione', style_bold_subtask)
				riga += 1
				for diz_sottotask in diz_issues[epic]['task'][task]['sotto_task']:
					sheet_task_aperti.write(riga, 1 + livello_sottotask, diz_sottotask['commessa_ot'], style)
					sheet_task_aperti.write(riga, 2 + livello_sottotask, diz_sottotask['key_issue'], style)
					sheet_task_aperti.write(riga, 3 + livello_sottotask, diz_sottotask['code_fg'],  style)
					sheet_task_aperti.write(riga, 4 + livello_sottotask, diz_sottotask['descrizione'], style)
					riga += 1

	# saving file
	path_to_save = jira_config.PATH_FILE + "ISSUE_APERTE"
	if not os.path.isdir(path_to_save):
		os.mkdir(path_to_save)
	book.save(path_to_save + "/issue_al_%s.xls"% date.today())
	print path_to_save + "/issue_al_%s.xls"% date.today()

if __name__ == '__main__':
	diz_issues = get_all_issue()
	pprint.pprint(diz_issues)
	create_excel(diz_issues)

