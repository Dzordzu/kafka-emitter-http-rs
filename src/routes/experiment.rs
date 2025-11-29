use crate::AppData;
use crate::models::{
    BeginResponse, EndRequest, EndResponse, ExperimentOverview, Insights, InsightsRequest,
    NewExperiment, RestoreExperiment,
};
use actix_web::{Responder, delete, get, post, web};
use uuid::Uuid;

#[utoipa::path(
    tag = "experiment",
    responses(
        (status = 200, description = "ID of the experiment", body = BeginResponse)
    )
)]
#[post("/")]
/// Create a new experiment
async fn begin(body: web::Json<NewExperiment>, data: web::Data<AppData>) -> impl Responder {
    let experiment_uuid = uuid::Uuid::new_v4();

    {
        let mut data = data.app_state.lock().await;

        data.new_experiment(experiment_uuid, body.listeners.clone())
            .await;
    }

    web::Json(BeginResponse { experiment_uuid })
}

#[utoipa::path(
    tag = "experiment",
    responses(
        (status = 200, description = "ID of the experiment", body = BeginResponse)
    )
)]
#[post("/restore")]
/// Restore existing experiment. Tries to read messages from the start. It won't work if
/// messages have been already read
async fn restore(body: web::Json<RestoreExperiment>, data: web::Data<AppData>) -> impl Responder {
    let mut data = data.app_state.lock().await;
    let experiment_uuid = body.experiment_uuid;

    data.restore_experiment(experiment_uuid, body.listeners.clone())
        .await;

    web::Json(BeginResponse { experiment_uuid })
}

#[utoipa::path(
    tag = "experiment",
    responses(
        (status = 200, description = "Delete all experiment data", body = BeginResponse)
    )
)]
#[delete("/")]
/// Delete experiment and its events data. Keep the messages
async fn end(body: web::Json<EndRequest>, data: web::Data<AppData>) -> impl Responder {
    let experiment_uuid = body.0.experiment_uuid;

    {
        let mut data = data.app_state.lock().await;
        data.end_experiment(experiment_uuid).await;
    }

    web::Json(EndResponse { experiment_uuid })
}

#[utoipa::path(
    tag = "experiment",
    responses(
        (status = 200, description = "Delete all experiment data from all experiments")
    )
)]
#[delete("/reset-all")]
/// Delete all experiments and their data
async fn reset(data: web::Data<AppData>) -> impl Responder {
    {
        let mut data = data.app_state.lock().await;

        let experiments_uuids: Vec<Uuid> = {
            let msg_state = data.messages_state.lock().await;
            msg_state.experiments.keys().cloned().collect()
        };

        for experiment_uuid in experiments_uuids {
            // Needed to stop consumers
            data.end_experiment(experiment_uuid).await;
        }

        // Just to make sure that there is nothing left
        let mut msg_state = data.messages_state.lock().await;
        msg_state.experiments.clear();
        msg_state.idx_message_to_experiment.0.clear();
        msg_state.idx_experiment_to_messages.0.clear();
        msg_state.messages.clear();
        msg_state.events.clear();

    }

    "Experiments cleared"
}

#[utoipa::path(
    tag = "experiment",
    responses(
        (status = 200, description = "List of registered experiments", body = Vec<Uuid>)
    )
)]
#[get("/list")]
/// Get all registered experiments
async fn list_experiments(data: web::Data<AppData>) -> impl Responder {
    let experiments = {
        data.app_state
            .lock()
            .await
            .messages_state
            .clone()
            .lock()
            .await
            .experiments
            .clone()
    };

    web::Json(experiments.keys().cloned().collect::<Vec<Uuid>>())
}

#[utoipa::path(
    tag = "experiment",
    params(
        InsightsRequest
    ),
    responses(
        (status = 200, description = "Get list of ", body = Insights)
    )
)]
#[get("/insights")]
/// Get all the details of the experiment
async fn get_insights(
    params: web::Query<InsightsRequest>,
    data: web::Data<AppData>,
) -> actix_web::Result<web::Json<Insights>> {
    let (experiment, messages, events) = data
        .experiment_related_data(&params.0.experiment_uuid)
        .await
        .ok_or(actix_web::error::ErrorNotFound("Experiment not found"))?;

    let messages = messages.0.into_values().collect();

    Ok(web::Json(Insights {
        messages,
        experiment,
        events,
    }))
}

#[utoipa::path(
    tag = "experiment",
    params(
        InsightsRequest
    ),
    responses(
        (status = 200, description = "Get list of ", body = ExperimentOverview)
    )
)]
#[get("/overview")]
/// Get general information about experiment
async fn get_config(
    params: web::Query<InsightsRequest>,
    data: web::Data<AppData>,
) -> actix_web::Result<web::Json<ExperimentOverview>> {
    let (experiment, messages, events) = data
        .experiment_related_data(&params.0.experiment_uuid)
        .await
        .ok_or(actix_web::error::ErrorNotFound("Experiment not found"))?;

    Ok(web::Json(ExperimentOverview {
        experiment,
        messages: messages.0.len(),
        events: events.len(),
    }))
}
