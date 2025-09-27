import pandas as pd
import json

def debug_transaction_linking():
    """Debug why transactions aren't linking to accounts properly"""
    
    print("🔍 TRANSACTION LINKING DEBUG")
    print("="*60)
    
    # Load raw data
    print("📂 Loading raw data...")
    
    # Accounts
    with open("api_data/accounts.json") as f:
        accounts_data = json.load(f)['results']
    print(f"✓ Accounts loaded: {len(accounts_data):,}")
    
    # Transfers/Transactions  
    with open("api_data/transfers.json") as f:
        transfers_data = json.load(f)['results']
    print(f"✓ Transfers loaded: {len(transfers_data):,}")
    
    # Extract IDs for comparison
    account_ids = set(acc['_id'] for acc in accounts_data)
    print(f"✓ Unique account IDs: {len(account_ids):,}")
    
    # Analyze transaction structure
    print(f"\n📋 TRANSACTION STRUCTURE ANALYSIS")
    print("-" * 40)
    
    if transfers_data:
        sample_transfer = transfers_data[0]
        print("Sample transfer record:")
        for key, value in sample_transfer.items():
            print(f"   • {key}: {value} ({type(value).__name__})")
    
    # Check transaction types
    transaction_types = {}
    payer_ids = set()
    payee_ids = set()
    
    for trans in transfers_data:
        # Count transaction types
        t_type = trans.get('type', 'unknown')
        transaction_types[t_type] = transaction_types.get(t_type, 0) + 1
        
        # Collect payer/payee IDs
        if 'payer_id' in trans and trans['payer_id']:
            payer_ids.add(trans['payer_id'])
        if 'payee_id' in trans and trans['payee_id']:
            payee_ids.add(trans['payee_id'])
    
    print(f"\n💸 TRANSACTION TYPE BREAKDOWN:")
    for t_type, count in transaction_types.items():
        print(f"   • {t_type}: {count:,} transactions")
    
    print(f"\n🆔 ID ANALYSIS:")
    print(f"   • Unique payer IDs: {len(payer_ids):,}")
    print(f"   • Unique payee IDs: {len(payee_ids):,}")
    print(f"   • Total unique transaction IDs: {len(payer_ids | payee_ids):,}")
    
    # Check ID overlaps
    payers_in_accounts = payer_ids & account_ids
    payees_in_accounts = payee_ids & account_ids
    
    print(f"\n🔗 ID MATCHING ANALYSIS:")
    print(f"   • Payer IDs that match account IDs: {len(payers_in_accounts):,}")
    print(f"   • Payee IDs that match account IDs: {len(payees_in_accounts):,}")
    print(f"   • Payer IDs NOT in accounts: {len(payer_ids - account_ids):,}")
    print(f"   • Payee IDs NOT in accounts: {len(payee_ids - account_ids):,}")
    
    # Show some examples of non-matching IDs
    unmatched_payers = list(payer_ids - account_ids)[:5]
    unmatched_payees = list(payee_ids - account_ids)[:5]
    
    if unmatched_payers:
        print(f"\n❌ Sample unmatched payer IDs:")
        for pid in unmatched_payers:
            print(f"   • {pid}")
    
    if unmatched_payees:
        print(f"\n❌ Sample unmatched payee IDs:")
        for pid in unmatched_payees:
            print(f"   • {pid}")
    
    # Show some matching examples
    matched_ids = list((payer_ids | payee_ids) & account_ids)[:5]
    if matched_ids:
        print(f"\n✅ Sample matching IDs:")
        for mid in matched_ids:
            print(f"   • {mid}")
    
    # Test specific transaction types
    print(f"\n🔬 DETAILED TYPE ANALYSIS:")
    
    for t_type in transaction_types.keys():
        type_transactions = [t for t in transfers_data if t.get('type') == t_type]
        if type_transactions:
            sample = type_transactions[0]
            print(f"\n   {t_type.upper()} sample:")
            print(f"      payer_id: {sample.get('payer_id', 'N/A')}")
            print(f"      payee_id: {sample.get('payee_id', 'N/A')}")
            print(f"      amount: {sample.get('amount', 'N/A')}")
            
            # Check if these IDs exist in accounts
            payer_exists = sample.get('payer_id') in account_ids if sample.get('payer_id') else False
            payee_exists = sample.get('payee_id') in account_ids if sample.get('payee_id') else False
            print(f"      payer_id in accounts: {payer_exists}")
            print(f"      payee_id in accounts: {payee_exists}")

def test_pandas_aggregation():
    """Test the pandas aggregation process step by step"""
    
    print(f"\n🧮 PANDAS AGGREGATION DEBUG")
    print("="*60)
    
    # Load and process like in main script
    with open("api_data/transfers.json") as f:
        transactions_raw = json.load(f)
    
    transactions_df = pd.json_normalize(transactions_raw['results'])
    print(f"📊 Transactions DataFrame shape: {transactions_df.shape}")
    print(f"📊 Columns: {list(transactions_df.columns)}")
    
    # Check if we have the expected columns
    expected_cols = ['type', 'amount', 'payer_id', 'payee_id', 'transaction_date', '_id']
    missing_cols = [col for col in expected_cols if col not in transactions_df.columns]
    if missing_cols:
        print(f"⚠️  Missing columns: {missing_cols}")
    
    # Filter columns if they exist
    available_cols = [col for col in expected_cols if col in transactions_df.columns]
    transactions_df = transactions_df[available_cols]
    
    # Convert amount to numeric
    if 'amount' in transactions_df.columns:
        transactions_df['amount'] = pd.to_numeric(transactions_df['amount'], errors='coerce').fillna(0)
    
    print(f"\n💸 Transaction type counts:")
    if 'type' in transactions_df.columns:
        type_counts = transactions_df['type'].value_counts()
        print(type_counts)
    
    # Test each aggregation
    if 'type' in transactions_df.columns and 'payee_id' in transactions_df.columns:
        print(f"\n🔍 Testing DEPOSIT aggregation...")
        deposits = transactions_df[transactions_df['type'] == 'deposit']
        print(f"   • Deposit records: {len(deposits):,}")
        if len(deposits) > 0:
            deposits_agg = deposits.groupby('payee_id').agg({
                '_id': 'count',
                'amount': 'sum'
            }).reset_index()
            print(f"   • Unique payee IDs in deposits: {len(deposits_agg):,}")
            print(f"   • Sample deposits aggregation:")
            print(deposits_agg.head())
    
    if 'type' in transactions_df.columns and 'payer_id' in transactions_df.columns:
        print(f"\n🔍 Testing WITHDRAWAL aggregation...")
        withdrawals = transactions_df[transactions_df['type'] == 'withdrawal']
        print(f"   • Withdrawal records: {len(withdrawals):,}")
        if len(withdrawals) > 0:
            withdrawals_agg = withdrawals.groupby('payer_id').agg({
                '_id': 'count', 
                'amount': 'sum'
            }).reset_index()
            print(f"   • Unique payer IDs in withdrawals: {len(withdrawals_agg):,}")
            print(f"   • Sample withdrawals aggregation:")
            print(withdrawals_agg.head())
    
    print(f"\n🔍 Testing P2P aggregation...")
    p2p = transactions_df[transactions_df['type'] == 'p2p']
    print(f"   • P2P records: {len(p2p):,}")
    if len(p2p) > 0 and 'payer_id' in p2p.columns:
        p2p_sent = p2p.groupby('payer_id').agg({
            '_id': 'count',
            'amount': 'sum'
        }).reset_index()
        print(f"   • Unique payer IDs in P2P: {len(p2p_sent):,}")
        print(f"   • Sample P2P sent aggregation:")
        print(p2p_sent.head())

def main():
    """Run all debugging tests"""
    
    debug_transaction_linking()
    test_pandas_aggregation()
    
    print(f"\n" + "="*60)
    print("🎯 DEBUGGING COMPLETE!")
    print("="*60)
    print("\n💡 NEXT STEPS:")
    print("1. Check if transaction IDs match account IDs")
    print("2. Verify transaction types are what we expect") 
    print("3. Look for data quality issues in ID fields")
    print("4. Consider if accounts and transactions are from different time periods")

if __name__ == "__main__":
    main()