#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask, render_template, url_for

from . import futures

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.temp.html', pathAppJs=url_for('static', filename='app.js'))

@app.route('/ajax/spider/start/', methods=['POST'])
def ajax():
    SHFE()
    return 'hello'