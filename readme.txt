Funzioni di questo package ed esempi di chiamata:



###########
##### estrarre i log da ot e creare il file da importare su mm
###########
python from_ot_to_mm.py  --date_from 2017/03/01 --date_to 2017/03/31 --email_utente alice.mazzoli@otconsulting.com --utente_mm mazzoli.a

###########
# generare il file con tutte le issue aperte o in progress
# nel file generato saranno visibili le epic, i task ed i sottotask associati
# tutto visibile in scala
###########
python import_issue.py

##########
# generare rendicontazione fine mese con i totali per persona e per commessa
##########
python importa_jira.py --date_from 2017/03/01 --date_to 2017/03/31 --mese Marzo --anno 2017 --operazione all

### COSA FARE A FINE MESE PER LA RENDICONTAZIONE
1. finire di inserire tutte le ore su ot jira
2. fare girare il batch from_ot_to_mm
3. eseguire le query di insert avendo premura però di saltare la propria settimana di operations
(occhio che dovrebbero combaciare le ore inserite su ot jira con quelle inserite su inserimento_jira
dal batch di MaxMara)

select jira_issue,sum(replace(replace(replace(worklog_time,'h',''),'30m','.5'),' ','') )
from inserimento_jira
where utente in ('mazzoli.a','beltrami.e','baronio.l','scinicariello.m')
and data like '2017-03%'
group by jira_issue

