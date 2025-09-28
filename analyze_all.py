import pandas as pd
import json
import logging

# --------------------------
# Logging setup
# --------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_json_data(file_path):
    """Load JSON data with error handling"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        results = data.get('results', [])
        logger.info(f"✓ Loaded {len(results)} records from {file_path}")
        return results
    except Exception as e:
        logger.error(f"✗ Error loading {file_path}: {e}")
        return []

def main():
    logger.info("🏦 Starting banking data processing with deposits, withdrawals, and P2P transfers...")

    # --------------------------
    # 1. Load JSON files
    # --------------------------
    accounts = pd.json_normalize(load_json_data("api_data/accounts.json"))
    bills = pd.json_normalize(load_json_data("api_data/bills.json"))
    transactions = pd.json_normalize(load_json_data("api_data/transfers.json"))
    customers = pd.json_normalize(load_json_data("api_data/customers.json"))

    # --------------------------
    # 2. Prepare customer data
    # --------------------------
    if not customers.empty:
        customers['zip5'] = customers.get('address.zip', None)
        customers['city'] = customers.get('address.city', None)
        customers['state'] = customers.get('address.state', None)
        keep_cols = ['_id', 'first_name', 'last_name', 'zip5', 'city', 'state']
        customers = customers[[col for col in keep_cols if col in customers.columns]]

    # --------------------------
    # 3. Aggregate bills
    # --------------------------
    if not bills.empty:
        bills['payment_amount'] = pd.to_numeric(bills['payment_amount'], errors='coerce').fillna(0)
        bills_summary = bills.groupby('account_id').agg(
            num_bills=pd.NamedAgg(column='_id', aggfunc='count'),
            num_recurring=pd.NamedAgg(column='status', aggfunc=lambda x: (x == 'recurring').sum()),
            total_bill_amount=pd.NamedAgg(column='payment_amount', aggfunc='sum'),
            avg_bill_amount=pd.NamedAgg(column='payment_amount', aggfunc='mean')
        ).reset_index()
    else:
        bills_summary = pd.DataFrame(columns=['account_id', 'num_bills', 'num_recurring', 'total_bill_amount', 'avg_bill_amount'])

    # --------------------------
    # 4. Separate transactions by type
    # --------------------------
    if not transactions.empty:
        transactions['amount'] = pd.to_numeric(transactions['amount'], errors='coerce').fillna(0)
        
        deposits = transactions[transactions['type'] == 'deposit']
        withdrawals = transactions[transactions['type'] == 'withdrawal']
        p2p = transactions[transactions['type'] == 'p2p']

        account_ids = set(accounts['_id'].tolist())

        # Internal vs external P2P
        p2p_internal = p2p[(p2p['payer_id'].isin(account_ids)) | (p2p['payee_id'].isin(account_ids))]
        outgoing = p2p_internal[p2p_internal['payer_id'].isin(account_ids)]
        incoming = p2p_internal[p2p_internal['payee_id'].isin(account_ids)]

        # Aggregate outgoing transfers
        outgoing_summary = outgoing.groupby('payer_id').agg(
            num_transfers_sent=pd.NamedAgg(column='_id', aggfunc='count'),
            total_transfers_sent=pd.NamedAgg(column='amount', aggfunc='sum'),
            avg_transfer_sent=pd.NamedAgg(column='amount', aggfunc='mean')
        ).reset_index().rename(columns={'payer_id': 'account_id'})

        # Aggregate incoming transfers
        incoming_summary = incoming.groupby('payee_id').agg(
            num_transfers_received=pd.NamedAgg(column='_id', aggfunc='count'),
            total_transfers_received=pd.NamedAgg(column='amount', aggfunc='sum'),
            avg_transfer_received=pd.NamedAgg(column='amount', aggfunc='mean')
        ).reset_index().rename(columns={'payee_id': 'account_id'})

        # Aggregate deposits
        deposits_summary = deposits.groupby('payee_id').agg(
            num_deposits=pd.NamedAgg(column='_id', aggfunc='count'),
            total_deposits=pd.NamedAgg(column='amount', aggfunc='sum'),
            avg_deposit=pd.NamedAgg(column='amount', aggfunc='mean')
        ).reset_index().rename(columns={'payee_id': 'account_id'})

        # Aggregate withdrawals
        withdrawals_summary = withdrawals.groupby('payer_id').agg(
            num_withdrawals=pd.NamedAgg(column='_id', aggfunc='count'),
            total_withdrawals=pd.NamedAgg(column='amount', aggfunc='sum'),
            avg_withdrawal=pd.NamedAgg(column='amount', aggfunc='mean')
        ).reset_index().rename(columns={'payer_id': 'account_id'})
    else:
        outgoing_summary = incoming_summary = deposits_summary = withdrawals_summary = pd.DataFrame(columns=['account_id'])

    # --------------------------
    # 5. Merge all metrics to accounts
    # --------------------------
    final_accounts = accounts.merge(bills_summary, left_on='_id', right_on='account_id', how='left')
    final_accounts = final_accounts.merge(outgoing_summary, on='account_id', how='left')
    final_accounts = final_accounts.merge(incoming_summary, on='account_id', how='left')
    final_accounts = final_accounts.merge(deposits_summary, on='account_id', how='left')
    final_accounts = final_accounts.merge(withdrawals_summary, on='account_id', how='left')

    # Fill missing numeric values
    for col in final_accounts.columns:
        if col.startswith(('num_', 'total_', 'avg_')):
            final_accounts[col] = final_accounts[col].fillna(0)

    # --------------------------
    # 6. Merge customer info
    # --------------------------
    final_accounts = final_accounts.merge(customers, left_on='customer_id', right_on='_id', how='left', suffixes=('', '_customer'))

    # --------------------------
    # 7. Compute derived metrics
    # --------------------------
    final_accounts['total_p2p_count'] = final_accounts['num_transfers_sent'] + final_accounts['num_transfers_received']
    final_accounts['total_p2p_volume'] = final_accounts['total_transfers_sent'] + final_accounts['total_transfers_received']
    final_accounts['total_transactions_count'] = (
        final_accounts['total_p2p_count'] + final_accounts['num_deposits'] + final_accounts['num_withdrawals'] + final_accounts['num_bills']
    )
    final_accounts['total_transaction_volume'] = (
        final_accounts['total_p2p_volume'] + final_accounts['total_deposits'] + final_accounts['total_withdrawals'] + final_accounts['total_bill_amount']
    )

    # --------------------------
    # 8. Aggregate per customer
    # --------------------------
    agg_cols = ['customer_id', 'first_name', 'last_name', 'zip5', 'city', 'state']
    agg_rules = {
        'balance': 'sum',
        'rewards': 'sum',
        'num_bills': 'sum',
        'num_recurring': 'sum',
        'num_transfers_sent': 'sum',
        'total_transfers_sent': 'sum',
        'num_transfers_received': 'sum',
        'total_transfers_received': 'sum',
        'num_deposits': 'sum',
        'total_deposits': 'sum',
        'num_withdrawals': 'sum',
        'total_withdrawals': 'sum',
        'total_p2p_count': 'sum',
        'total_p2p_volume': 'sum',
        'total_transactions_count': 'sum',
        'total_transaction_volume': 'sum'
    }

    customer_summary = final_accounts.groupby(agg_cols, as_index=False).agg(agg_rules)
        # --------------------------
    # 10. Insights
    # --------------------------
    logger.info("📊 Generating insights...")

    insights = {}

    # Average per customer
    insights['avg_deposits_per_customer'] = {
        "count": customer_summary['num_deposits'].mean(),
        "amount": customer_summary['total_deposits'].mean()
    }
    insights['avg_withdrawals_per_customer'] = {
        "count": customer_summary['num_withdrawals'].mean(),
        "amount": customer_summary['total_withdrawals'].mean()
    }
    insights['avg_p2p_sent_per_customer'] = {
        "count": customer_summary['num_transfers_sent'].mean(),
        "amount": customer_summary['total_transfers_sent'].mean()
    }
    insights['avg_p2p_received_per_customer'] = {
        "count": customer_summary['num_transfers_received'].mean(),
        "amount": customer_summary['total_transfers_received'].mean()
    }
    insights['avg_total_transaction_volume'] = customer_summary['total_transaction_volume'].mean()

    # Totals across dataset
    insights['totals'] = {
        "deposits": customer_summary['total_deposits'].sum(),
        "withdrawals": customer_summary['total_withdrawals'].sum(),
        "p2p_sent": customer_summary['total_transfers_sent'].sum(),
        "p2p_received": customer_summary['total_transfers_received'].sum(),
        "bills": customer_summary['total_transaction_volume'].sum()
    }

    # Ratios
    customer_summary['deposit_withdrawal_ratio'] = customer_summary.apply(
        lambda row: row['total_deposits'] / row['total_withdrawals']
        if row['total_withdrawals'] > 0 else None,
        axis=1
    )
    insights['avg_deposit_withdrawal_ratio'] = customer_summary['deposit_withdrawal_ratio'].mean()

    # Print insights
    print("\n===== INSIGHTS =====")
    for k, v in insights.items():
        print(f"{k}: {v}")

    # --------------------------
    # 9. Output
    # --------------------------
    print(customer_summary.head())
    customer_summary.to_csv('customer_full_transaction_summary.csv', index=False)
    logger.info("✅ Full customer transaction summary saved as 'customer_full_transaction_summary.csv'")

    return customer_summary

if __name__ == "__main__":
    main()
