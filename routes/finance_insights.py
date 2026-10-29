"""Shared finance taxonomy and currency-separated reporting."""
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
import math

CATEGORIES = {
    'Bank Charges': ['Local transaction fees', 'International transfer fees', 'Account & statement fees'],
    'Samples': ['Buyer sample products', 'Buyer sample delivery'],
    'Transport': ['Local transport', 'Shipment transport', 'Company travel'],
    'Donations': ['Religious donations', 'Charitable donations'],
    'Company Administration & Compliance': ['VAT', 'IRD filings', 'Company secretary & director changes', 'Printing & stationery', 'Equipment & maintenance'],
    'Petty Cash': ['Allocations', 'Top-ups', 'Reimbursements'],
    'Import': ['Imported goods', 'Import freight', 'Customs & clearance'],
    'Export': ['Export sales', 'Export freight', 'Certificates & documentation'],
    'Production': ['Raw materials', 'Manufacturing', 'Packaging materials', 'Labels & design', 'Supplier advances'],
    'Capital': ['Owner contributions', 'Capital withdrawals'],
    'Loans': ['Loans given', 'Loans received', 'Loan repayments'],
}
# A transaction direction does not change the business classification.
EXPENSE_CATEGORIES = CATEGORIES
INCOME_CATEGORIES = CATEGORIES

ALIASES = {
    'local transport': ('Transport', 'Local transport'),
    'hala transport': ('Transport', 'Shipment transport'),
    'export shipping charges fcl': ('Export', 'Export freight'),
    'export shipping charges fcl | export shipping charges fcl': ('Export', 'Export freight'),
    'jars import clearance payment 1': ('Import', 'Customs & clearance'),
    'packing materials': ('Production', 'Labels & design'),
    'card board box payment': ('Production', 'Packaging materials'),
    'heat seal and ziplock cost': ('Production', 'Packaging materials'),
    'labels': ('Production', 'Labels & design'),
    'label': ('Production', 'Labels & design'),
    'label sample desing mask city': ('Production', 'Labels & design'),
    'label sample design mask city': ('Production', 'Labels & design'),
    'label charges': ('Production', 'Labels & design'),
    'pytho cetificate': ('Export', 'Certificates & documentation'),
    'hala consignment certificate fee': ('Export', 'Certificates & documentation'),
    'jayabima / misrat 2 - consignment cetificat': ('Export', 'Certificates & documentation'),
    'vat': ('Company Administration & Compliance', 'VAT'),
    'nill return submit to ird': ('Company Administration & Compliance', 'IRD filings'),
    'secretary': ('Company Administration & Compliance', 'Company secretary & director changes'),
    'directer change': ('Company Administration & Compliance', 'Company secretary & director changes'),
    'formations': ('Company Administration & Compliance', 'Company secretary & director changes'),
    'hardware material cost': ('Company Administration & Compliance', 'Equipment & maintenance'),
    'printing': ('Company Administration & Compliance', 'Printing & stationery'),
    'spice': ('Production', 'Raw materials'),
    'jayabima spice mnanufaturing final cost': ('Production', 'Manufacturing'),
    'jayabima spcie pvt ltd': ('Production', 'Manufacturing'),
    'heat seal advace estimate no 7943': ('Production', 'Supplier advances'),
    'heat seal balance payment invoice no 7943': ('Production', 'Packaging materials'),
    'anual retune peti cash': ('Petty Cash', ''),
    'peti cash small transport': ('Petty Cash', ''),
    'peti cash': ('Petty Cash', ''),
    'personal work': ('Loans', 'Loans given'),
    'personal work (loan)': ('Loans', 'Loans given'),
    'advance payment': ('Production', 'Supplier advances'),
    'donation': ('Donations', ''),
    'swameen wahanse': ('Donations', 'Religious donations'),
    'swameen wahanse book': ('Donations', 'Religious donations'),
    'ramanayake impression': ('Production', 'Labels & design'),
    'deposit': ('Capital', 'Owner contributions'),
    'spice/ export and import': ('Export', 'Export sales'),
    'misrat': ('Export', 'Export sales'),
    'misrat 2nd payment - export': ('Export', 'Export sales'),
}


def category_path(row):
    raw = (row.get('category') or '').strip()
    key = ' '.join(raw.lower().split())
    if 'subcategory' in row or row.get('original_category') is not None:
        return raw, row.get('subcategory') or '', 'unclassified' if raw == 'Needs classification' else 'structured'
    if key in ('spice samples', 'king coconut samples', 'samples'):
        description = (row.get('description') or '').lower()
        if any(word in description for word in ('packaging', 'labell')):
            return 'Production', 'Labels & design' if 'labell' in description else 'Packaging materials', 'suggested'
        delivery = any(word in description for word in ('shipment', 'sending', 'freight'))
        return 'Samples', 'Buyer sample delivery' if delivery else '', 'suggested'
    if key in ALIASES:
        return *ALIASES[key], 'suggested'
    if key in ('bank charges', 'bank - (unknown)', 'remittence') or key.startswith('monthly bank transaction fees'):
        description = (row.get('description') or '').lower()
        child = ''
        if key == 'remittence' or 'remittance' in description or 'remittence' in description:
            child = 'International transfer fees'
        elif any(word in description for word in ('statement', 'user fee', 'check book', 'cheque book')):
            child = 'Account & statement fees'
        elif key.startswith('monthly') or 'monthly bank transaction' in description:
            child = 'Local transaction fees'
        return 'Bank Charges', child, 'suggested'
    if key == 'import':
        description = (row.get('description') or '').lower()
        child = 'Customs & clearance' if 'broker' in description else 'Import freight' if 'shipping' in description else 'Imported goods' if 'glass jar' in description else ''
        return 'Import', child, 'suggested'
    if raw in CATEGORIES:
        return raw, '', 'structured'
    if ' / ' in raw:
        group, child = raw.split(' / ', 1)
        return group.strip(), child.strip(), 'structured'
    return 'Needs classification', '', 'unclassified'


def build_analysis(rows, currency, start=None, end=None, basis='all'):
    monthly = defaultdict(lambda: {'income': Decimal(0), 'outgoing': Decimal(0), 'count': 0})
    categories = defaultdict(lambda: {'amount': Decimal(0), 'count': 0, 'children': defaultdict(Decimal)})
    incoming = outgoing = Decimal(0)
    count = pending = fallback_dates = omitted = suggested = unclassified = review = 0
    for row in rows:
        if row.get('deleted_at') or row.get('currency') != currency or row.get('type') not in ('INCOME', 'OUTCOME'):
            continue
        settled = (row['type'] == 'INCOME' and row.get('status') == 'RECEIVED') or (row['type'] == 'OUTCOME' and row.get('status') == 'PAID')
        if basis == 'settled' and not settled:
            continue
        used_created = not (row.get('paid_date') or '').strip()
        value = row.get('created_at') if used_created else row.get('paid_date')
        try:
            recorded = date.fromisoformat(str(value)[:10])
            amount = Decimal(str(row.get('amount')))
            if not amount.is_finite() or amount < 0 or not math.isfinite(float(amount)):
                raise ValueError()
        except (ValueError, TypeError, InvalidOperation):
            omitted += 1
            continue
        if (start and recorded < start) or (end and recorded > end):
            continue
        count += 1
        review += bool(row.get('category_review'))
        pending += not settled
        fallback_dates += used_created
        point = monthly[recorded.isoformat()[:7]]
        point['count'] += 1
        if row['type'] == 'INCOME':
            incoming += amount
            point['income'] += amount
        else:
            outgoing += amount
            point['outgoing'] += amount
            group, child, source = category_path(row)
            suggested += source == 'suggested'
            unclassified += source == 'unclassified'
            categories[group]['amount'] += amount
            categories[group]['count'] += 1
            categories[group]['children'][child or 'No subcategory'] += amount
    # Fill gaps for a truthful time axis; bound the period to 1200 months.
    keys = sorted(monthly)
    if keys:
        first = date.fromisoformat(keys[0] + '-01')
        last = date.fromisoformat(keys[-1] + '-01')
        while first <= last and len(monthly) <= 1200:
            monthly[first.isoformat()[:7]]
            if first >= last:
                break
            first = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return {
        'currency': currency, 'basis': basis, 'count': count,
        'income': float(incoming), 'outgoing': float(outgoing), 'net_movement': float(incoming - outgoing),
        'unsettled_count': pending, 'created_date_count': fallback_dates, 'omitted_count': omitted,
        'review_count': review, 'suggested_count': suggested, 'unclassified_count': unclassified,
        'monthly': [{'period': key, 'income': float(value['income']), 'outgoing': float(value['outgoing']), 'count': value['count']} for key, value in sorted(monthly.items())],
        'categories': [{'name': key, 'amount': float(value['amount']), 'count': value['count'],
                        'children': [{'name': name, 'amount': float(amount)} for name, amount in sorted(value['children'].items(), key=lambda item: item[1], reverse=True)]}
                       for key, value in sorted(categories.items(), key=lambda item: item[1]['amount'], reverse=True)],
    }
