use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};
use spl_associated_token_account::{
    get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use std::str::FromStr;

// Programme ID pour le Token-2022 sur Solana
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
// Programme ID pour le SPL Token classique
const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Détecte si le token utilise le standard Token-2022 ou le SPL Token classique
/// en vérifiant le programme propriétaire du mint auprès de la blockchain.
pub fn get_token_program_for_mint(rpc_client: &RpcClient, mint_pubkey: &Pubkey) -> Pubkey {
    if let Ok(account_data) = rpc_client.get_account(mint_pubkey) {
        let owner_str = account_data.owner.to_string();
        if owner_str == TOKEN_2022_PROGRAM_ID {
            return Pubkey::from_str(TOKEN_2022_PROGRAM_ID).unwrap();
        }
    }
    // Par défaut, on retourne le SPL Token classique
    Pubkey::from_str(SPL_TOKEN_PROGRAM_ID).unwrap()
}

/// Fonction principale pour exécuter l'ordre d'achat avec gestion des ATA et du Blockhash frais
pub fn execute_buy_transaction(
    rpc_url: &str,
    payer: &Keypair,
    mint_pubkey: &Pubkey,
    buy_instructions: Vec<Instruction>, // Vos instructions spécifiques à Pump.fun ou Raydium
) -> Result<String, Box<dyn std::error::Error>> {
    // 1. Initialiser le client RPC (ex: Helius)
    let rpc_client = RpcClient::new_with_commitment(
        rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    );

    // 2. Déterminer automatiquement le bon programme de token (Classique ou Token-2022)
    let token_program_id = get_token_program_for_mint(&rpc_client, mint_pubkey);

    // 3. Calculer l'adresse du compte de token associé (ATA) avec le bon programme
    let associated_token_address = get_associated_token_address_with_program_id(
        &payer.pubkey(),
        mint_pubkey,
        &token_program_id,
    );

    let mut instructions = vec![];

    // 4. Vérifier si le compte ATA existe déjà, sinon ajouter l'instruction de création idempotente
    if rpc_client.get_account(&associated_token_address).is_err() {
        let create_ata_ix = create_associated_token_account_idempotent(
            &payer.pubkey(),
            &payer.pubkey(),
            mint_pubkey,
            &token_program_id, // Corrige l'erreur IncorrectProgramId
        );
        instructions.push(create_ata_ix);
    }

    // 5. Ajouter les instructions d'achat transmises en paramètre
    instructions.extend(buy_instructions);

    // 6. Récupérer un Blockhash frais IMMÉDIATEMENT avant de signer (Résout "Blockhash not found")
    let (recent_blockhash, _last_valid_block_height) = rpc_client
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())?;

    // 7. Construire et signer la transaction
    let mut transaction = Transaction::new_with_payer(&instructions, Some(&payer.pubkey()));
    transaction.sign(&[payer], recent_blockhash);

    // 8. Envoyer la transaction sur le réseau
    let signature = rpc_client.send_and_confirm_transaction(&transaction)?;

    Ok(signature.to_string())
}
