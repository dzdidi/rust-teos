use std::sync::MutexGuard;
use std::sync::{Arc, Mutex};

use lightning::chain::chainmonitor::Persist;
use lightning::sign::InMemorySigner;
use lightning::chain::transaction::OutPoint;
use lightning::chain::channelmonitor::{ChannelMonitor, ChannelMonitorUpdate};
use lightning::chain::ChannelMonitorUpdateStatus;

use teos_common::appointment::{Appointment, Locator};
use teos_common::TowerId;
use teos_common::{cryptography, errors};

use crate::convert::CommitmentRevocation;
use crate::http::AddAppointmentError;
use crate::net::http;
use crate::wt_client::{RevocationData, WTClient};
use crate::TowerStatus;

pub struct WtConnector {
    pub wt_client: Arc<Mutex<WTClient>>,
}

impl WtConnector {
    /// Sends an appointment to all registered towers for every new commitment transaction.
    ///
    /// The appointment is built using the data provided by the backend (dispute txid and penalty transaction).
    // TODO:
    // - either move this into WT_Client
    pub async fn on_commitment_revocation(
        &self,
        commitment_revocation: CommitmentRevocation,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log::debug!(
            "New commitment revocation received for channel {}. Commit number {}",
            commitment_revocation.channel_id,
            commitment_revocation.commit_num
        );

        // TODO: For now, to_self_delay is hardcoded to 42. Revisit and define it better / remove it when / if needed
        let locator = Locator::new(commitment_revocation.commitment_txid);
        let appointment = Appointment::new(
            locator,
            cryptography::encrypt(
                &commitment_revocation.penalty_tx,
                &commitment_revocation.commitment_txid,
            )
            .unwrap(),
            42,
        );
        let signature =
            cryptography::sign(&appointment.to_vec(), &self.wt_client.lock().unwrap().user_sk);

        // Looks like we cannot iterate through towers given a locked state is not Send (due to the async call),
        // so we need to clone the bare minimum.
        let towers = self.wt_client
            .lock()
            .unwrap()
            .towers
            .iter()
            .map(|(id, info)| (*id, info.net_addr.clone(), info.status))
            .collect::<Vec<_>>();

        for (tower_id, net_addr, status) in towers {
            if status.is_reachable() {
                match http::add_appointment(tower_id, &net_addr, &appointment, &signature).await {
                    Ok((slots, receipt)) => {
                        self.wt_client
                            .lock()
                            .unwrap()
                            .add_appointment_receipt(tower_id, locator, slots, &receipt);
                        log::debug!("Response verified and data stored in the database");
                    }
                    Err(e) => match e {
                        AddAppointmentError::RequestError(e) => {
                            if e.is_connection() {
                                log::warn!(
                                    "{tower_id} cannot be reached. Adding {} to pending appointments",
                                    appointment.locator
                                );
                                let mut state = self.wt_client.lock().unwrap();
                                state.set_tower_status(tower_id, TowerStatus::TemporaryUnreachable);
                                state.add_pending_appointment(tower_id, &appointment);

                                self.send_to_retrier(&state, tower_id, appointment.locator);
                            }
                        }
                        AddAppointmentError::ApiError(e) => match e.error_code {
                            errors::INVALID_SIGNATURE_OR_SUBSCRIPTION_ERROR => {
                                log::warn!(
                                    "There is a subscription issue with {tower_id}. Adding {} to pending",
                                    appointment.locator
                                );
                                let mut state = self.wt_client.lock().unwrap();
                                state.set_tower_status(tower_id, TowerStatus::SubscriptionError);
                                state.add_pending_appointment(tower_id, &appointment);
                                self.send_to_retrier(&state, tower_id, appointment.locator);
                            }

                            _ => {
                                log::warn!(
                                    "{tower_id} rejected the appointment. Error: {}, error_code: {}",
                                    e.error,
                                    e.error_code
                                );
                                self.wt_client
                                    .lock()
                                    .unwrap()
                                    .add_invalid_appointment(tower_id, &appointment);
                            }
                        },
                        AddAppointmentError::SignatureError(proof) => {
                            log::warn!("Cannot recover known tower_id from the appointment receipt. Flagging tower as misbehaving");
                            self.wt_client
                                .lock()
                                .unwrap()
                                .flag_misbehaving_tower(tower_id, proof)
                        }
                    },
                };
            } else if status.is_misbehaving() {
                log::warn!("{tower_id} is misbehaving. Not sending any further appointments",);
            } else {
                if status.is_subscription_error() {
                    log::warn!(
                        "There is a subscription issue with {tower_id}. Adding {} to pending",
                        appointment.locator
                    );
                } else {
                    log::warn!(
                        "{tower_id} is {status}. Adding {} to pending",
                        appointment.locator,
                    );
                }

                let mut state = self.wt_client.lock().unwrap();
                state.add_pending_appointment(tower_id, &appointment);

                if !status.is_unreachable() {
                    self.send_to_retrier(&state, tower_id, appointment.locator);
                }
            }
        }

        Ok(())
    }

    /// Sends fresh data to a retrier as long as is does not exist, or it does and its running.
    pub fn send_to_retrier(&self, state: &MutexGuard<WTClient>, tower_id: TowerId, locator: Locator) {
        if if let Some(status) = state.get_retrier_status(&tower_id) {
            // A retrier in the retriers map can only be running or idle
            status.is_running()
        } else {
            true
        } {
            state
                .unreachable_towers
                .send((tower_id, RevocationData::Fresh(locator)))
                .unwrap();
        } else {
            log::debug!("Not sending data to idle retrier ({tower_id}, {locator})")
        }
    }
}

impl Persist<InMemorySigner> for WtConnector {
    fn persist_new_channel(
        &self,
        channel_funding_outpoint: OutPoint,
        monitor: &ChannelMonitor<InMemorySigner>,
    ) -> ChannelMonitorUpdateStatus {
        // todo build the commitment revocation
        // call the same logic as in on_commitment_revocation

        return ChannelMonitorUpdateStatus::Completed;
    }

    fn update_persisted_channel(
        &self,
        channel_funding_outpoint: OutPoint,
        monitor_update: Option<&ChannelMonitorUpdate>,
        monitor: &ChannelMonitor<InMemorySigner>,
    ) -> ChannelMonitorUpdateStatus {
        // todo build the commitment revocation
        // call the same logic as in on_commitment_revocation

        return ChannelMonitorUpdateStatus::Completed;
    }

    fn archive_persisted_channel(&self, channel_funding_outpoint: OutPoint) {
        // do nothing?
    }
}
