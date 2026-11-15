import os
import logging
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
)

# Configurazione dei log
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Legge il token dalle variabili d'ambiente di Render
TOKEN = os.getenv("TOKEN")
ADMIN_ID = 7935524220

# Saldo fittizio disponibile sul conto (0.00 Euro)
SALDO_DISPONIBILE = 0.00

# Stati per la conversazione
GET_TRADERS, GET_BUDGET, GET_DURATION = range(3)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Non sei autorizzato a usare questo bot.")
        return ConversationHandler.END
    
    await update.message.reply_text(
        "Benvenuto nel Bot di Copy-Trading!\n\n"
        "Ottimo, sessione aperta! Quali sono i nomi dei trader da seguire? (es. Trader1, Trader2)"
    )
    return GET_TRADERS

async def get_traders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    traders = [t.strip() for t in text.replace(',', ' ').split() if len(t.strip()) > 0]
    context.user_data['traders'] = traders
    
    await update.message.reply_text(
        "Ottimo! Qual è il budget totale in euro da allocare per questa sessione? (es. 500)"
    )
    return GET_BUDGET

async def get_budget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        budget = float(update.message.text.strip().replace(',', '.'))
        
        if budget > SALDO_DISPONIBILE:
            await update.message.reply_text(
                f"Non hai {budget} euro.\n"
                f"Nel tuo saldo hai {SALDO_DISPONIBILE:.2f} euro.\n\n"
                f"Sessione interrotta per fondi insufficienti."
            )
            return ConversationHandler.END
            
        context.user_data['budget'] = budget
        
        await update.message.reply_text(
            "Per quante ore devo mantenere attiva la sessione? (es. 0.5 per mezz'ora o 3):"
        )
        return GET_DURATION
    except ValueError:
        await update.message.reply_text("Per favore, inserisci un numero valido per il budget (es. 500):")
        return GET_BUDGET

async def get_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text_input = update.message.text.strip().replace(',', '.')
        hours = float(text_input)
        context.user_data['duration'] = hours
        
        traders_list = context.user_data.get('traders', [])
        traders_str = ", ".join(traders_list) if traders_list else "Nessuno"
        budget_val = context.user_data.get('budget', 0)
        
        await update.message.reply_text(
            f"Sessione avviata con successo!\n\n"
            f"- Trader seguiti: {traders_str}\n"
            f"- Budget: {budget_val} Euro\n"
            f"- Durata impostata: {hours} ore\n\n"
            f"Puoi scrivermi 'resoconto' per vedere il bilancio o /stop per chiudere."
        )
        return ConversationHandler.END
    except Exception as e:
        await update.message.reply_text("Inserisci un valore numerico valido per la durata in ore (es. 0.5 o 2):")
        return GET_DURATION

async def resoconto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "Resoconto Attuale\n\n"
        "• Saldo disponibile: 0.00 Euro\n"
        "• Profitto/Perdita stimato: +0.00 Euro\n"
        "• Stato: Inattivo / Bloccato per fondi"
    )

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operazione annullata o sessione chiusa.")
    return ConversationHandler.END

# Server HTTP fittizio per Render
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is active 24/7!")

def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    server.serve_forever()

def main():
    if not TOKEN:
        print("ERRORE: Variabile d'ambiente TOKEN non trovata!")
        return

    # Avvia il server web fittizio in background
    server_thread = threading.Thread(target=run_web_server, daemon=True)
    server_thread.start()

    # Assicura che esista un event loop per il thread principale
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    application = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CommandHandler('inizia', start),
            MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex("(?i)^(inizia|ciao|start|parti|ricomincia)$"), start)
        ],
        states={
            GET_TRADERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_traders)],
            GET_BUDGET: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_budget)],
            GET_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_duration)],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('stop', cancel),
            MessageHandler(filters.Regex("(?i)^(stop|esci|chiudi)$"), cancel)
        ]
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler('resoconto', resoconto))
    application.add_handler(MessageHandler(filters.Regex("(?i)^resoconto$"), resoconto))
    application.add_handler(CommandHandler('stop', cancel))

    print("Application started")
    application.run_polling()

if __name__ == '__main__':
    main()
