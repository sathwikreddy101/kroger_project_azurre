from flask import Flask, render_template, request, redirect, url_for, session
import pymssql
import pandas as pd
import os
import plotly.express as px
import plotly.io as pio

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Change to a secure key

UPLOAD_FOLDER = './uploads'  # Directory to store uploaded files
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def get_db_connection():
    try:
        conn = pymssql.connect(
            server='krogerdata.database.windows.net',
            user='sathwik',
            password='Test@123',
            database='krogerdata'
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None


# Home route that shows a login form
@app.route('/')
def home():
    if 'username' in session:
        return redirect(url_for('navigation'))  # Redirect to navigation if logged in
    return render_template('login.html')

# Login route to handle form submission
@app.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    password = request.form['password']
    email = request.form['email']
    
    # You can add user authentication here if needed
    session['username'] = username  # Store user info in session
    return redirect(url_for('navigation'))  # Redirect to navigation page

# Logout route to end session
@app.route('/logout')
def logout():
    session.pop('username', None)  # Clear user session
    return redirect(url_for('home'))  # Redirect to login page

# Navigation route
@app.route('/navigation')
def navigation():
    if 'username' not in session:
        return redirect(url_for('home'))  # Redirect to login if not logged in
    return render_template('navigation.html')

# Route to view data
@app.route('/view_data', methods=['GET', 'POST'])
def view_data():
    if 'username' not in session:
        return redirect(url_for('home'))  # Redirect to login if not logged in
    
    conn = get_db_connection()
    cursor = conn.cursor()

    # Handle search functionality (POST request)
    if request.method == 'POST':
        search_query = request.form.get('hshd_num')  # Get the search value

        # Filter data based on Hshd_num if search_query is provided
        if search_query:
            query = f"""
            SELECT 
                H.*, 
                T.*, 
                P.*
            FROM [dbo].[households] H
            JOIN [dbo].[transactions] T ON H.Hshd_num = T.Hshd_num
            JOIN [dbo].[400_products] P ON T.Basket_num = P.Product_num
            WHERE H.Hshd_num = {search_query}
            ORDER BY T.Hshd_num, T.Basket_num, T.PURCHASE_DATE, P.Product_num, P.Department, P.COMMODITY;
            """
        else:
            query = """
            SELECT 
                H.*, 
                T.*, 
                P.*
            FROM [dbo].[households] H
            JOIN [dbo].[transactions] T ON H.Hshd_num = T.Hshd_num
            JOIN [dbo].[400_products] P ON T.Basket_num = P.Product_num
            ORDER BY T.Hshd_num, T.Basket_num, T.PURCHASE_DATE, P.Product_num, P.Department, P.COMMODITY;
            """
    else:
        # For GET request, load all data
        query = """
        SELECT 
            H.*, 
            T.*, 
            P.*
        FROM [dbo].[households] H
        JOIN [dbo].[transactions] T ON H.Hshd_num = T.Hshd_num
        JOIN [dbo].[400_products] P ON T.Basket_num = P.Product_num
        ORDER BY T.Hshd_num, T.Basket_num, T.PURCHASE_DATE, P.Product_num, P.Department, P.COMMODITY;
        """

    cursor.execute(query)
    data = cursor.fetchall()

    # Get column names dynamically from the result
    column_names = [column[0] for column in cursor.description]
    
    cursor.close()
    conn.close()

    return render_template('view_data.html', data=data, column_names=column_names)

# Route for other functionalities like adding data, dashboard, etc.
@app.route('/add_data')
def add_data():
    if 'username' not in session:
        return redirect(url_for('home'))  # Redirect to login if not logged in
    return render_template('add_data.html')


# Route to upload and load data
@app.route('/upload_data', methods=['GET', 'POST'])
def upload_data():
    if 'username' not in session:
        return redirect(url_for('home'))  # Redirect to login if not logged in
    
    if request.method == 'POST':
        file_type = request.form['file_type']
        file = request.files['file']
        
        if file and file.filename.endswith('.csv'):
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(file_path)  # Save the uploaded file
            
            # Load data into database
            load_data_to_db(file_path, file_type)
            return "File uploaded and data loaded successfully!"
        else:
            return "Invalid file format. Please upload a CSV file."
    
    return render_template('upload_data.html')

# Function to load data into the database
def load_data_to_db(file_path, file_type):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Load CSV file using pandas
    data = pd.read_csv(file_path)

    # Determine the target table based on file type
    if file_type == 'transactions':
        table_name = 'dbo.transactions'
    elif file_type == 'households':
        table_name = 'dbo.households'
    elif file_type == 'products':
        table_name = '[dbo].[400_products]'
    else:
        return "Invalid file type."

    # Insert data into the database
    for _, row in data.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.index)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

# Dashboard route to display the main dashboard page
@app.route('/dashboard')
def dashboard():
    if 'username' not in session:
        return redirect(url_for('home'))  # Redirect to login if not logged in
    
    # Queries for Retail Questions
    demographics_data = query_demographics()
    spending_over_time_data = query_spending_over_time()
    basket_analysis_data = query_basket_analysis()
    seasonal_trends_data = query_seasonal_trends()
    brand_preferences_data = query_brand_preferences()

    # Plot graphs using Plotly (or another JS library)
    spending_graph = create_spending_graph(spending_over_time_data)
    basket_graph = create_basket_graph(basket_analysis_data)
    seasonal_graph = create_seasonal_graph(seasonal_trends_data)
    brand_graph = create_brand_graph(brand_preferences_data)

    # Render the dashboard page with the data and graphs
    return render_template('dashboard.html', 
                           demographics_data=demographics_data,
                           spending_graph=spending_graph,
                           basket_graph=basket_graph,
                           seasonal_graph=seasonal_graph,
                           brand_graph=brand_graph)

def query_demographics():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        HH_SIZE, 
        CHILDREN,  
        INCOME_RANGE, 
        COUNT(*) AS EngagementCount
    FROM households
    GROUP BY HH_SIZE, CHILDREN, INCOME_RANGE
    """

    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    return pd.DataFrame(data, columns=['HouseholdSize', 'PresenceOfChildren', 'Income', 'EngagementCount'])

def query_spending_over_time():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        YEAR(T.PURCHASE_DATE) AS Year,
        SUM(T.SPEND) AS TotalSpent
    FROM transactions T
    GROUP BY YEAR(T.PURCHASE_DATE)
    """

    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    return pd.DataFrame(data, columns=['Year', 'TotalSpent'])

def query_basket_analysis():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        P.COMMODITY, 
        COUNT(*) AS ProductCombinationCount
    FROM transactions T
    JOIN [dbo].[400_products] P ON T.Product_NUM = P.Product_NUM
    GROUP BY P.COMMODITY
    """

    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    return pd.DataFrame(data, columns=['ProductCategory', 'ProductCombinationCount'])

def query_seasonal_trends():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        MONTH(T.PURCHASE_DATE) AS Month, 
        SUM(T.SPEND) AS TotalSpent
    FROM transactions T
    GROUP BY MONTH(T.PURCHASE_DATE)
    """

    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    return pd.DataFrame(data, columns=['Month', 'TotalSpent'])

def query_brand_preferences():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        P.Brand_TY, 
        COUNT(*) AS PurchaseCount
    FROM transactions T
    JOIN [dbo].[400_products] P ON T.Product_NUM = P.Product_NUM
    GROUP BY P.Brand_TY
    """

    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()

    return pd.DataFrame(data, columns=['Brand', 'PurchaseCount'])

# Function to create spending graph
def create_spending_graph(df):
    fig = px.line(df, x='Year', y='TotalSpent', title='Spending Over Time')
    return pio.to_html(fig, full_html=False)

# Function to create basket analysis graph
def create_basket_graph(df):
    fig = px.bar(df, x='ProductCategory', y='ProductCombinationCount', title='Basket Analysis (Product Combinations)')
    return pio.to_html(fig, full_html=False)

# Function to create seasonal trends graph
def create_seasonal_graph(df):
    fig = px.line(df, x='Month', y='TotalSpent', title='Seasonal Trends')
    return pio.to_html(fig, full_html=False)

# Function to create brand preferences graph
def create_brand_graph(df):
    fig = px.bar(df, x='Brand', y='PurchaseCount', title='Brand Preferences')
    return pio.to_html(fig, full_html=False)



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
