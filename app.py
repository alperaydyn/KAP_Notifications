from flask import Flask, json, render_template, render_template_string, redirect, url_for, jsonify, request, session
from flask_session import Session
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import kap_reader

app = Flask(__name__, static_folder='static', template_folder='templates')
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_TYPE'] = "filesystem"
Session(app)

DB_PATH = 'kap.db'

@app.route('/')
def main_():
    return render_template("kap_page.html")

@app.route('/soru')
def soru():
    return render_template_string('Soru')

@app.route('/plan')
def plan():
    return render_template_string('Plan')


"""
    if config.get('companies'): cr.read_companies()
    if config.get('last_notif'): print(cd.get_last_notification())
    if config.get('read_notif'): print(cr.get_notification(config.get('read_notif')))
    if config.get('read_notifs'): cr.get_notifications(config.get('read_notifs'))
    if config.get('missing_notifs'): cr.get_missing_notifications()
    if config.get('refresh_existing'): cr._refresh_all_notifications(
        int(config.get('refresh_existing')[0]), int(config.get('refresh_existing')[1]))
    if config.get('display_random'): cr.display_random_notifs(int(config.get('display_random')))
    """

@app.route('/createdb')
def db_create():
    cd = kap_reader.database()
    cd.create_database()
    return 'ok'