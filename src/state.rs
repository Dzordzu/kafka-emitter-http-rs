use crate::consumers::Consumers;
use crate::models::{Experiment, KafkaBrokerCfg, Message, MessageEvent};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

#[derive(Default, Debug, Clone)]
pub struct ExperimentToMessagesIdx(pub HashMap<Uuid, Vec<Uuid>>);

#[derive(Default, Debug, Clone)]
pub struct MessageToExperimentIdx(pub HashMap<Uuid, Uuid>);

#[derive(Debug, Clone)]
pub struct State {
    /// Used to spawn, manage and destroy kafka consumers (receivers)
    pub consumers: Consumers,
    pub messages_state: Arc<Mutex<MessagesState>>,
}

/// Message uuid to message
#[derive(Debug, Clone, Default)]
pub struct MessageMapping(pub HashMap<Uuid, Message>);

#[derive(Debug, Clone, Default)]
pub struct MessagesState {
    pub experiments: HashMap<Uuid, Experiment>,

    /// Experiment uuid to message mapping
    pub messages: HashMap<Uuid, MessageMapping>,

    /// Experiment uuid to message events map
    pub events: HashMap<Uuid, Vec<MessageEvent>>,

    pub idx_experiment_to_messages: ExperimentToMessagesIdx,
    pub idx_message_to_experiment: MessageToExperimentIdx,
}

impl MessagesState {
    pub fn insert_message(&mut self, message: Message, experiment_uuid: Uuid) {
        if self.experiments.contains_key(&experiment_uuid) {
            self.idx_message_to_experiment
                .0
                .insert(message.uuid, experiment_uuid);

            self.idx_experiment_to_messages
                .0
                .entry(experiment_uuid)
                .or_default()
                .push(message.uuid);

            self.messages
                .entry(experiment_uuid)
                .or_default()
                .0
                .insert(message.uuid, message);

            self.events.entry(experiment_uuid).or_default();
        } else {
            tracing::warn!("Unknown experiment: {experiment_uuid}");
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    pub fn new() -> Self {
        let messages_state = Arc::new(Mutex::new(Default::default()));
        Self {
            consumers: Consumers::new(messages_state.clone()),
            messages_state,
        }
    }

    pub async fn new_experiment(
        &mut self,
        uuid: Uuid,
        consumers: Vec<KafkaBrokerCfg>,
    ) -> &mut Self {
        info!("Starting new experiment with uuid {}", uuid);

        {
            self.messages_state
                .lock()
                .await
                .experiments
                .insert(uuid, Experiment::new(uuid, consumers.clone()));
        }

        for consumer in consumers {
            self.consumers.start(uuid, consumer, false).await;
        }

        self
    }

    pub async fn restore_experiment(
        &mut self,
        uuid: Uuid,
        consumers: Vec<KafkaBrokerCfg>,
    ) -> &mut Self {
        info!("Starting new experiment with uuid {}", uuid);

        {
            self.messages_state
                .lock()
                .await
                .experiments
                .insert(uuid, Experiment::new(uuid, consumers.clone()));
        }

        for consumer in consumers {
            self.consumers.start(uuid, consumer, true).await;
        }

        self
    }

    pub async fn end_experiment(&mut self, uuid: Uuid) -> &mut Self {
        info!("Stopping experiment {}", uuid);

        self.consumers.stop(&uuid);

        {
            let mut state = self.messages_state.lock().await;
            state.experiments.remove(&uuid);
            state.events.remove(&uuid);
            if let Some(messages) = state.idx_experiment_to_messages.0.remove(&uuid) {
                for message in messages {
                    state.idx_message_to_experiment.0.remove(&message);
                    state.messages.remove(&message);
                }
            }
        }

        self
    }

    // pub async fn get_experiment_stats(&self, uuid: Uuid) -> Stats {
    // }
}
