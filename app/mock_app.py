import streamlit as st
import requests


def fetch(session, url):
    try:
        result = session.get(url)
        return result.json()
    except Exception:
        return {}


def main():
    st.set_page_config(page_title="Transaction Entry", page_icon="🤖")
    st.title("Mock Transaction Input")
    session = requests.Session()
    with st.form("trx_form"):
        display = ("Eduard", "Sherlie","Putri","Roy","Danang")
        options = list(range(1,len(display)+1))
        account_id = st.selectbox('Account',options,format_func=lambda x: display[x-1])
        merchant_type = st.selectbox('Merchant',("RESTO","ELECTRONICS","FASHION"))
        amount = st.slider('Amount', 0, 5000, 25)

        submitted = st.form_submit_button("Submit")

        if submitted:
            st.write("Result")
            data = requests.post("http://bjb-workshop-cfm-nifi2.se-indo.a465-9q4k.cloudera.site:8787/transaction", json={'account_id' : f'{account_id}','merchant_type' : f'{merchant_type}','amount' : f'{amount}'})
            if data:
                st.write(data.json())
            else:
                st.error("Error")


if __name__ == '__main__':
    main()