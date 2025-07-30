import streamlit as st

# --- Page Config: Tab Title & Icon ---
st.set_page_config(
    page_title="Connecting Filecoin VM to any Blockchain via Axelar",
    page_icon="https://img.cryptorank.io/coins/filecoin1602761898881.png",
    layout="wide"
)

# --- Title with Logo ---
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://img.cryptorank.io/coins/filecoin1602761898881.png" alt="Filecoin vm" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Connecting Filecoin VM to any Blockchain via Axelar</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Info Box ---
st.markdown(
    """
    <div style="background-color: #0090ff; padding: 15px; border-radius: 10px; border: 1px solid #0090ff;">
        Axelar serves as Filecoin’s primary interoperability network, allowing decentralized storage on any blockchain in Web3 via FVM. 
        With FVM’s mainnet launch on March 14, 2023, Axelar’s infrastructure enables developers to build applications that access verifiable 
        storage across multiple blockchains, ensuring cost-effective and seamless access for users and developers.
</div>
    """,
    unsafe_allow_html=True
)

# --- Reference and Rebuild Info ---
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1856738793325268992/OouKI10c_400x400.jpg" alt="Flipside" style="width:25px; height:25px; border-radius: 50%;">
            <span>Data Powered by: <a href="https://flipsidecrypto.xyz/home/" target="_blank">Flipside</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Links with Logos ---
st.markdown(
    """
    <div style="font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/logo.png" alt="Axelar" style="width:20px; height:20px;">
            <a href="https://www.axelar.network/" target="_blank">https://www.axelar.network/</a>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://axelarscan.io/logos/accounts/squid.svg" alt="Squid" style="width:20px; height:20px;">
            <a href="https://www.squidrouter.com/" target="_blank">https://www.squidrouter.com/</a>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://cdn-icons-png.flaticon.com/512/5968/5968958.png" alt="X" style="width:20px; height:20px;">
            <a href="https://x.com/axelar" target="_blank">https://x.com/axelar</a>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://cdn-icons-png.flaticon.com/512/5968/5968958.png" alt="X" style="width:20px; height:20px;">
            <a href="https://x.com/squidrouter" target="_blank">https://x.com/squidrouter</a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)
