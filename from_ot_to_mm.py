import importa_jira
import pprint
import os
import argparse
import csv
import config as jira_config
from operator import itemgetter
from itertools import groupby

def export_log_from_ot(utente_ot, utente_mm, date_from, date_to):
	jira_obj = importa_jira.JiraExtractor({'date_from': date_from,
								'date_to': date_to })

	lista_issues = jira_obj.get_all_issue_by_projects()
	diz_totale_issue_time = {}
	lista_worklog_on_issues = []
	for issue in lista_issues:
		diz_totale_issue_time[issue] = {}
		lista_worklog_on_issues.extend(jira_obj.get_worked_log_by_issue_from_date(issue))

	work_utente = []
	for log in lista_worklog_on_issues:
		if log['author'] == utente_ot:
			diz_per_mm = {}
			diz_per_mm['key_jira_mm'] = log['issue'].raw['fields']['customfield_10040']
			diz_per_mm['data'] = log['date']
			diz_per_mm['time_spent'] = log['time_spent']
			work_utente.append(diz_per_mm)


	grouper = itemgetter("data")
	work_utente = sorted(work_utente, key=grouper)
	diz_group = {}
	for data, g in groupby(work_utente, grouper):
		lista_straordinari = []
		lista_ordinari = []
		grouped = list(g)
		tot_ore = sum(i['time_spent'] for i in grouped)
		if tot_ore > 8:
			h_straordinario = tot_ore - 8
			# di sicuro le ore di straordinario combaciano con una loggata
			# perche' dobbiamo dichiararle di "straordinario"
			for i in grouped:
				if int(i['time_spent']) == h_straordinario:
					lista_straordinari.append(i)
				else:
					lista_ordinari.append(i)
		else:
			lista_ordinari.extend(grouped)

		diz_group[data] = {}
		diz_group[data]['straordinari'] = lista_straordinari
		diz_group[data]['ordinari'] = lista_ordinari


	path_to_save = jira_config.PATH_FILE + "ISSUE_APERTE"
	if not os.path.isdir(path_to_save):
		os.mkdir(path_to_save)
	myfile = open(path_to_save + "/log_from_ot.txt", 'w')
	lista_values = []
	for data in diz_group:
		# per inserire utente-data-jira sono in chiave quindi raggruppo anche per codice_jira
		grouper = itemgetter("key_jira_mm")
		diz_group[data]['ordinari'] = sorted(diz_group[data]['ordinari'], key=grouper)
		for key_jira_mm, g in groupby(diz_group[data]['ordinari'], grouper):
			grouped = list(g)
			tot_ore = sum(i['time_spent'] for i in grouped)
			lista_values.append("('{0}', '{1}', '{2}', '{3}h', '', '')".format(utente_mm,
																					data,
																					key_jira_mm,
																					tot_ore))

		for item2 in diz_group[data]['straordinari']:
			print 'Straordinari: Data {0} , issue {1}, ore {2} '.format(data,
																		  item2['key_jira_mm'],
																		  item2['time_spent'])
	stringa_totale = '''
	insert into #replace_this_with_table_name#
		(utente, data, jira_issue, worklog_time, descrizione, flag_inserito)
	values
		{} '''.format(',\n'.join(lista_values))
	myfile.write(stringa_totale)
	print path_to_save + "/log_from_ot.txt"


if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		description='''
		#################### ####################
		Script per estrarre le ore loggate su OT e quindi creare
		il file da importare su maxmara
		#################### ####################
			''')

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

	parser.add_argument('--email_utente',
						dest='email_utente',
						help='formato ex: alice.mazzoli@otconsulting.com',
						required=True
						)
	parser.add_argument('--utente_mm',
						dest='utente_mm',
						help='formato ex: mazzoli.a',
						required=True
						)


	namespace = parser.parse_args()

	diz = {'date_from': namespace.date_from,
		   'date_to': namespace.date_to ,
		   }

	export_log_from_ot(namespace.email_utente, namespace.utente_mm, namespace.date_from, namespace.date_to)
