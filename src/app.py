#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import signal

from flask import Flask, render_template, url_for

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.temp.html',
            pathAppJs=url_for('static', filename='app.js'),
            pathAppCss=url_for('static', filename='app.css'),
        )

