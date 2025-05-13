import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


df = pd.read_csv('gs://online-payments-fraud/destination/cleaned_data/cleaned_transaction_data1.csv') 
df['time_stamp'] = pd.to_datetime(df['time_stamp'])


sns.set(style='whitegrid')

#Transaction Amount Distribution (Fraud vs Non-Fraud)
plt.figure(figsize=(10, 5))
sns.histplot(data=df, x='transaction_amount', hue='fraud_label', bins=40, palette=['skyblue', 'salmon'], kde=True)
plt.title('Transaction Amount Distribution by Fraud Label')
plt.xlabel('Transaction Amount')
plt.ylabel('Count')
plt.legend(labels=['Non-Fraud', 'Fraud'])
plt.tight_layout()
plt.show()
plt.savefig('/tmp/fraud_vs_non-fraud.png')
#Monthly Fraud Trend
df['month'] = df['time_stamp'].dt.to_period('M').astype(str)
monthly_fraud = df.groupby('month')['fraud_label'].sum().reset_index()

plt.figure(figsize=(10, 5))
sns.lineplot(data=monthly_fraud, x='month', y='fraud_label', marker='o', color='crimson')
plt.title('Monthly Fraud Transaction Trend')
plt.xlabel('Month')
plt.ylabel('Number of Fraud Transactions')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
plt.savefig('/tmp/fraud_trend.png')
#Fraud Count by Merchant Category (Top 10)
merchant_fraud = df.groupby('merchant_category')['fraud_label'].sum().sort_values(ascending=False).head(10).reset_index()
plt.figure(figsize=(10, 5))
sns.barplot(data=merchant_fraud, x='merchant_category', y='fraud_label', hue='merchant_category', palette='viridis', legend=False)
plt.title('Top 10 Merchant Categories by Fraud Count')
plt.xlabel('Merchant Category')
plt.ylabel('Fraud Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
plt.savefig('/tmp/fraud_rate_merchant.png')
#Fraud Count by Device Type
device_fraud = df.groupby('device_type')['fraud_label'].sum().sort_values(ascending=False).reset_index()

plt.figure(figsize=(8, 4))
sns.barplot(data=device_fraud, x='device_type', y='fraud_label', hue='device_type', palette='coolwarm', legend=False)
plt.title('Fraud Count by Device Type')
plt.xlabel('Device Type')
plt.ylabel('Fraud Count')
plt.tight_layout()
plt.show()
plt.savefig('/tmp/fraud_count_device.png')
