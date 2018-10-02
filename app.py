#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import signal

from flask import Flask, render_template, url_for

from futures import SHFE

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.temp.html',
            pathAppJs=url_for('static', filename='app.js'),
            pathAppCss=url_for('static', filename='app.css'),
        )

@app.route('/ajax/spider/start/', methods=['POST'])
def ajax():
    shfe = SHFE()

    def handler(signal, frame):
        shfe.saveTable()
        sys.exit(0)

    signal.signal(signal.SIGINT, handler)

    try:
        shfe.startSpider()
    finally:
        shfe.saveTable()

    return 'hello'

