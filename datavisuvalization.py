import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set visual style
sns.set(style='whitegrid')

# Load data
df = pd.read_csv('cleaned_data.csv')

# Fix 1: Date parsing - remove dayfirst, since format is ISO-like
df['time_stamp'] = pd.to_datetime(df['time_stamp'], errors='coerce', format='%Y-%m-%d %H:%M:%S')

# Drop unparseable timestamps
df = df.dropna(subset=['time_stamp'])

# Add time features
df['month'] = df['time_stamp'].dt.to_period('M').astype(str)
df['hour'] = df['time_stamp'].dt.hour

#Transaction Amount Distribution
plt.figure(figsize=(10, 5))
sns.histplot(data=df, x='transaction_amount', hue='fraud_label', bins=40,
             palette='pastel', kde=True)
plt.title('Transaction Amount Distribution by Fraud Label')
plt.xlabel('Transaction Amount')
plt.ylabel('Count')
plt.legend(labels=['Non-Fraud', 'Fraud'])
plt.tight_layout()
plt.savefig('/tmp/fraud_vs_non-fraud.png')
plt.close()

#Monthly Fraud Transaction Trend
monthly_fraud = df.groupby('month', observed=True)['fraud_label'].sum().reset_index()
plt.figure(figsize=(10, 5))
sns.lineplot(data=monthly_fraud, x='month', y='fraud_label', marker='o', color='crimson')
plt.title('Monthly Fraud Transaction Trend')
plt.xlabel('Month')
plt.ylabel('Number of Fraud Transactions')

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/tmp/fraud_trend.png')
plt.close()

#Top 10 Merchant Categories by Fraud Count 
merchant_fraud = (df.groupby('merchant_category')['fraud_label']
                    .sum().sort_values(ascending=False).head(10).reset_index())
plt.figure(figsize=(10, 5))
sns.barplot(data=merchant_fraud, x='merchant_category', y='fraud_label',
            hue='merchant_category', palette='viridis', legend=False)
plt.title('Top 10 Merchant Categories by Fraud Count')
plt.xlabel('Merchant Category')
plt.ylabel('Fraud Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/tmp/fraud_rate_merchant.png')
plt.close()

#Fraud Count by Device Type 
device_fraud = df.groupby('device_type')['fraud_label'].sum().reset_index().sort_values(by='fraud_label', ascending=False)
plt.figure(figsize=(8, 4))
sns.barplot(data=device_fraud, x='device_type', y='fraud_label',
            hue='device_type', palette='coolwarm', legend=False)
plt.title('Fraud Count by Device Type')
plt.xlabel('Device Type')
plt.ylabel('Fraud Count')
plt.tight_layout()
plt.savefig('/tmp/fraud_count_device.png')
plt.close()

# Transaction Count per Hour of Day 
plt.figure(figsize=(10, 5))
sns.countplot(data=df, x='hour', hue='fraud_label', palette='Set2')
plt.title('Transactions per Hour of Day (Fraud vs Non-Fraud)')
plt.xlabel('Hour')
plt.ylabel('Transaction Count')
plt.legend(title='Fraud Label')
plt.tight_layout()
plt.savefig('/tmp/fraud_by_hour.png')
plt.close()

#Fraud Count by Age Group 
df['age_group'] = pd.cut(df['card_age'], bins=[0, 30, 90, 180, 365, 1000],
                         labels=['<30d', '30–90d', '90–180d', '180–365d', '1yr+'])
age_fraud = df.groupby('age_group', observed=True)['fraud_label'].sum().reset_index()
plt.figure(figsize=(8, 4))
sns.barplot(data=age_fraud, x='age_group', y='fraud_label', hue='age_group', palette='cubehelix', legend=False)
plt.title('Fraud Count by Card Age Group')
plt.xlabel('Card Age Group')
plt.ylabel('Fraud Count')
plt.tight_layout()
plt.savefig('/tmp/fraud_by_age_group.png')
plt.close()

#Fraud Count by Card Type 
card_fraud = df.groupby('card_type')['fraud_label'].sum().reset_index()
plt.figure(figsize=(8, 4))
sns.barplot(data=card_fraud, x='card_type', y='fraud_label', hue='card_type', palette='Blues', legend=False)
plt.title('Fraud Count by Card Type')
plt.xlabel('Card Type')
plt.ylabel('Fraud Count')
plt.tight_layout()
plt.savefig('/tmp/fraud_by_card_type.png')
plt.close()

#Fraud Rate by Authentication Method 
auth_method_fraud = df.groupby('authentication_method')['fraud_label'].mean().reset_index()
plt.figure(figsize=(8, 4))
sns.barplot(data=auth_method_fraud, x='authentication_method', y='fraud_label',
            hue='authentication_method', palette='rocket', legend=False)
plt.title('Fraud Rate by Authentication Method')
plt.xlabel('Authentication Method')
plt.ylabel('Fraud Rate')
plt.tight_layout()
plt.savefig('/tmp/fraud_by_authentication.png')
plt.close()
