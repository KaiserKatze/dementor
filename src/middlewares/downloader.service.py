#!/usr/bin/env python
# -*- coding: utf-8 -*-

class DownloaderMiddleware:
    def process_request(self, callback):
        """Handle HTTP request"""
        callback(self.session)

    def process_response(self):
        """Handle HTTP response"""
