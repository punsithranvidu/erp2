"""Render the actual screens without production database credentials."""
from html.parser import HTMLParser
from pathlib import Path
import unittest
from flask import Flask, render_template

ROOT = Path(__file__).resolve().parents[1]


class Elements(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            self.ids.append(attrs['id'])
        if tag == 'a':
            self.links.append(attrs)


class WorkspaceTemplateTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'))

    def render(self, name, role='ADMIN', user='Example'):
        with self.app.test_request_context('/dashboard'):
            return render_template(name, user=user, role=role, year=2026, month=10)

    def test_every_module_has_navigation_skip_target_and_unique_html_ids(self):
        for path in (ROOT / 'templates').glob('*.html'):
            if path.name in ('login.html', 'service_unavailable.html'):
                continue
            for role in ('ADMIN', 'EMP'):
                with self.subTest(page=path.name, role=role):
                    html = self.render(path.name, role)
                    parser = Elements()
                    parser.feed(html.split('<script>')[0])
                    self.assertIn('appSidebar', parser.ids)
                    self.assertIn('app-content', parser.ids)
                    self.assertEqual(len(parser.ids), len(set(parser.ids)))
                    self.assertIn('css/workspace.css', html)
                    self.assertIn('js/workspace.js', html)

    def test_employee_navigation_starts_hidden_until_permissions_load(self):
        parser = Elements()
        parser.feed(self.render('emp_dashboard.html', 'EMP'))
        links = [link for link in parser.links if 'data-nav-module' in link]
        self.assertEqual(len(links), 15)
        self.assertTrue(all('hidden' in link for link in links))

    def test_existing_badge_and_permission_hooks_remain(self):
        for name in ('admin_dashboard.html', 'emp_dashboard.html'):
            html = self.render(name)
            for identifier in ('weeklyTasksBadge', 'msgBadge', 'calendarBadge', 'notesBadge'):
                self.assertIn(f'id="{identifier}"', html)
        html = self.render('emp_dashboard.html', 'EMP')
        self.assertIn('data-mod="FINANCE"', html)
        self.assertIn('window.erpWorkspaceMe', html)

    def test_user_names_are_escaped(self):
        html = self.render('admin_dashboard.html', user='<script>alert(1)</script>')
        self.assertNotIn('<script>alert(1)</script>', html)
        self.assertIn('&lt;script&gt;', html)

    def test_login_remains_separate(self):
        html = self.render('login.html')
        self.assertNotIn('appSidebar', html)
        self.assertNotIn('workspace.css', html)
