"""===============================================================================

        FILE: tests/test___gdrive___spreadsheets.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-02-25T19:23:14.658479
    REVISION: ---
==============================================================================="""

from alex_leontiev_toolbox_python.gdrive.spreadsheets import (
    download_df_from_google_sheets,
    get_creds,
)


def test_download_df_from_google_sheets():
    creds = get_creds(client_secret_file="client_secret.json", create_if_not_exist=True)
    df = download_df_from_google_sheets(
        creds, "1FwMJvc8MFIF8JXKQ-7d2l4AvkhnzAMm5ZgG7HaKToUk", sheet_name="unmerged"
    )
    df.to_csv("/tmp/53eb0f85_29d1_4e3f_933a_44a1c0361df3.csv", index=None)
    # print(df)
