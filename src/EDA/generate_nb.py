import nbformat as nbf
import os

def create_eda_notebook():
    nb = nbf.v4.new_notebook()

    cells = []

    # Title and Introduction
    cells.append(nbf.v4.new_markdown_cell("# Basic Exploratory Data Analysis: MOBIE Datasets\n"
                                         "This notebook performs structural analysis and general distribution plotting for the Portuguese EV charging station datasets."))

    # Imports
    cells.append(nbf.v4.new_code_cell("import pandas as pd\n"
                                     "import matplotlib.pyplot as plt\n"
                                     "import seaborn as sns\n"
                                     "import os\n"
                                     "\n"
                                     "# Set plot style\n"
                                     "sns.set_theme(style='whitegrid')\n"
                                     "plt.rcParams['figure.figsize'] = (12, 6)"))

    # Data Path Configuration
    cells.append(nbf.v4.new_code_cell("DATA_DIR = '../../data'\n"
                                     "POSTOS_FILE = os.path.join(DATA_DIR, 'MOBIe_Lista_de_postos.csv')\n"
                                     "TARIFAS_FILE = os.path.join(DATA_DIR, 'MOBIE_Tarifas.csv')"))

    # Loading Datasets
    cells.append(nbf.v4.new_code_cell("# Load datasets\n"
                                     "df_postos = pd.read_csv(POSTOS_FILE, sep=';', encoding='utf-8-sig')\n"
                                     "df_tarifas = pd.read_csv(TARIFAS_FILE, sep=';', encoding='utf-8-sig')\n"
                                     "\n"
                                     "print('Datasets loaded successfully.')"))

    # Structural Analysis Helper
    cells.append(nbf.v4.new_code_cell("def print_structure(df, name):\n"
                                     "    print(f'\\n--- Structure of {name} ---')\n"
                                     "    print(f'Shape: {df.shape}')\n"
                                     "    print('\\nColumn Info:')\n"
                                     "    print(df.info())\n"
                                     "    print('\\nFirst 5 rows:')\n"
                                     "    display(df.head())\n"
                                     "\n"
                                     "print_structure(df_postos, 'MOBIe_Lista_de_postos')\n"
                                     "print_structure(df_tarifas, 'MOBIE_Tarifas')"))

    # Distribution Plotting Helper
    cells.append(nbf.v4.new_code_cell("def plot_distributions(df, df_name):\n"
                                     "    print(f'\\nProcessing distributions for {df_name}...')\n"
                                     "    for col in df.columns:\n"
                                     "        plt.figure(figsize=(10, 5))\n"
                                     "        \n"
                                     "        # Determine if categorical or numerical\n"
                                     "        if df[col].dtype == 'object' or df[col].nunique() < 20:\n"
                                     "            # Categorical Plot\n"
                                     "            order = df[col].value_counts().index[:15]  # Top 15 categories for readability\n"
                                     "            sns.countplot(data=df, y=col, order=order, palette='viridis')\n"
                                     "            plt.title(f'Top Categories Distribution: {col}')\n"
                                     "        else:\n"
                                     "            # Numerical Plot\n"
                                     "            try:\n"
                                     "                sns.histplot(df[col].dropna(), kde=True, color='teal')\n"
                                     "                plt.title(f'Numerical Distribution: {col}')\n"
                                     "            except Exception as e:\n"
                                     "                print(f'Could not plot numerical distribution for {col}: {e}')\n"
                                     "                continue\n"
                                     "        \n"
                                     "        plt.xlabel('Count / Frequency')\n"
                                     "        plt.ylabel(col)\n"
                                     "        plt.tight_layout()\n"
                                     "        plt.show()") )

    # Execute Plotting
    cells.append(nbf.v4.new_code_cell("# Plot distributions for both datasets\n"
                                     "plot_distributions(df_postos, 'MOBIe_Lista_de_postos')"))

    cells.append(nbf.v4.new_code_cell("plot_distributions(df_tarifas, 'MOBIE_Tarifas')"))

    nb['cells'] = cells

    with open('/home/joao-martins/Desktop/code/Enable Mobility/code/Enable/src/EDA/eda_basic.ipynb', 'w') as f:
        nbf.write(nb, f)

if __name__ == '__main__':
    create_eda_notebook()
