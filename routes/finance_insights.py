"""Category choices and currency-separated reporting over existing ledger rows."""
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
import math

EXPENSE_CATEGORIES = {
    'Transport': ['Local deliveries', 'Shipment transport', 'Company travel'],
    'Shipping & clearance': ['Freight', 'Customs clearance', 'Handling & forwarding'],
    'Samples': ['Sample products', 'Sample packaging', 'Sample courier'],
    'Bank fees': ['Local bank charges', 'International transfer fees', 'Account & statement fees'],
    'Products & packaging': ['Product purchases', 'Bottles & jars', 'Labels & printing', 'Packing materials'],
    'Company operations': ['Administration', 'Printing & stationery', 'Equipment & maintenance'],
    'Compliance & taxes': ['Certifications', 'Company-secretary fees', 'Taxes & filings'],
    'Transfers & advances': ['Petty cash allocations', 'Supplier advances', 'Personal loans'],
    'Donations': ['Donations & contributions'],
}
INCOME_CATEGORIES = {
    'Sales': ['Export sales', 'Local sales', 'Sample sales'],
    'Funding & transfers': ['Owner capital', 'Loans received', 'Account transfers'],
    'Other receipts': ['Refunds', 'Other income'],
}

# Explicit aliases only. Ambiguous combined descriptions remain visible for review.
ALIASES = {
    'local transport': ('Transport', 'Local deliveries'),
    'hala transport': ('Transport', 'Shipment transport'),
    'export shipping charges fcl': ('Shipping & clearance', 'Freight'),
    'export shipping charges fcl | export shipping charges fcl': ('Shipping & clearance', 'Freight'),
    'jars import clearance payment 1': ('Shipping & clearance', 'Customs clearance'),
    'packing materials': ('Products & packaging', 'Packing materials'),
    'card board box payment': ('Products & packaging', 'Packing materials'),
    'heat seal and ziplock cost': ('Products & packaging', 'Packing materials'),
    'labels': ('Products & packaging', 'Labels & printing'),
    'label': ('Products & packaging', 'Labels & printing'),
    'label charges': ('Compliance & taxes', 'Certifications'),
    'pytho cetificate': ('Compliance & taxes', 'Certifications'),
    'hala consignment certificate fee': ('Compliance & taxes', 'Certifications'),
    'jayabima / misrat 2 - consignment cetificat': ('Compliance & taxes', 'Certifications'),
    'vat': ('Compliance & taxes', 'Taxes & filings'),
    'nill return submit to ird': ('Compliance & taxes', 'Taxes & filings'),
    'secretary': ('Compliance & taxes', 'Company-secretary fees'),
    'directer change': ('Compliance & taxes', 'Company-secretary fees'),
    'formations': ('Compliance & taxes', 'Company-secretary fees'),
    'hardware material cost': ('Company operations', 'Equipment & maintenance'),
    'printing': ('Company operations', 'Printing & stationery'),
    'spice': ('Products & packaging', 'Product purchases'),
    'jayabima spice mnanufaturing final cost': ('Products & packaging', 'Product purchases'),
    'heat seal advace estimate no 7943': ('Transfers & advances', 'Supplier advances'),
    'heat seal balance payment invoice no 7943': ('Products & packaging', 'Packing materials'),
    'anual retune peti cash': ('Transfers & advances', 'Petty cash allocations'),
    'peti cash small transport': ('Transfers & advances', 'Petty cash allocations'),
    'peti cash': ('Transfers & advances', 'Petty cash allocations'),
    'personal work': ('Transfers & advances', 'Personal loans'),
    'personal work (loan)': ('Transfers & advances', 'Personal loans'),
    'advance payment': ('Transfers & advances', 'Supplier advances'),
    'spice samples': ('Samples', 'Sample products'),
    'king coconut samples': ('Samples', 'Sample products'),
    'samples': ('Samples', 'Sample products'),
    'donation': ('Donations', 'Donations & contributions'),
    'swameen wahanse': ('Donations', 'Donations & contributions'),
}


def category_path(row):
    raw = (row.get('category') or '').strip()
    key = ' '.join(raw.lower().split())
    if row.get('type') == 'OUTCOME' and key in ALIASES:
        return *ALIASES[key], 'suggested'
    if ' / ' in raw:
        group, child = raw.split(' / ', 1)
        if group.strip() and child.strip():
            return group.strip(), child.strip(), 'structured'
    key = ' '.join(raw.lower().split())
    if row.get('type') == 'INCOME':
        aliases = {'deposit': ('Funding & transfers', 'Owner capital'),
                   'spice samples': ('Sales', 'Sample sales'), 'samples': ('Sales', 'Sample sales')}
        if key in aliases:
            return *aliases[key], 'suggested'
    else:
        if key in ('bank charges', 'bank - (unknown)', 'remittence') or key.startswith('monthly bank transaction fees'):
            description = (row.get('description') or '').lower()
            if key == 'remittence' or 'remittance' in description or 'remittence' in description:
                child = 'International transfer fees'
            elif any(word in description for word in ('statement', 'user fee', 'check book', 'cheque book')):
                child = 'Account & statement fees'
            else:
                child = 'Local bank charges'
            return 'Bank fees', child, 'suggested'
        description = (row.get('description') or '').lower()
        if key == 'import' and description.strip() == 'glass jar import':
            return 'Products & packaging', 'Bottles & jars', 'suggested'
        if key == 'import' and 'jar shipping cost' in description:
            return 'Shipping & clearance', 'Freight', 'suggested'
        if key == 'ramanayake impression' and description.strip() == 'labels':
            return 'Products & packaging', 'Labels & printing', 'suggested'
        if key == 'jayabima spcie pvt ltd' and 'spice' in description:
            return 'Products & packaging', 'Product purchases', 'suggested'
    return 'Needs classification', raw or 'Unspecified', 'unclassified'


def build_analysis(rows, currency, start=None, end=None, basis='all'):
    monthly = defaultdict(lambda: {'income': Decimal(0), 'outgoing': Decimal(0), 'count': 0})
    categories = defaultdict(lambda: {'amount': Decimal(0), 'count': 0, 'children': defaultdict(Decimal)})
    incoming = outgoing = Decimal(0)
    count = pending = fallback_dates = omitted = suggested = unclassified = 0
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
            categories[group]['children'][child] += amount
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
        'suggested_count': suggested, 'unclassified_count': unclassified,
        'monthly': [{'period': key, 'income': float(value['income']), 'outgoing': float(value['outgoing']), 'count': value['count']} for key, value in sorted(monthly.items())],
        'categories': [{'name': key, 'amount': float(value['amount']), 'count': value['count'],
                        'children': [{'name': name, 'amount': float(amount)} for name, amount in sorted(value['children'].items(), key=lambda item: item[1], reverse=True)]}
                       for key, value in sorted(categories.items(), key=lambda item: item[1]['amount'], reverse=True)],
    }
