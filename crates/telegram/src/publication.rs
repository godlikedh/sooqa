use std::error::Error;

use async_trait::async_trait;
use sooqa_library::MediaKind;
use teloxide::{
    payloads::{
        CopyMessageSetters, SendAnimationSetters, SendAudioSetters, SendPhotoSetters,
        SendVideoSetters,
    },
    prelude::Requester,
    types::{ChatId, FileId, InputFile, MessageId, ParseMode},
};

use crate::{TelegramApiError, TeloxideApi};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TelegramPublicationRequest {
    pub target_chat_id: i64,
    pub storage_chat_id: i64,
    pub storage_message_id: i64,
    pub telegram_file_id: Option<String>,
    pub media_kind: MediaKind,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub disable_notification: bool,
}

#[async_trait]
pub trait TelegramPublicationApi: Clone + Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    async fn copy_from_storage(
        &self,
        request: &TelegramPublicationRequest,
    ) -> Result<i64, Self::Error>;

    async fn send_storage_file(
        &self,
        request: &TelegramPublicationRequest,
    ) -> Result<i64, Self::Error>;

    fn is_copy_unavailable(error: &Self::Error) -> bool;

    fn is_known_caption_error(error: &Self::Error) -> bool;

    fn is_retryable_no_effect(error: &Self::Error) -> bool;

    fn is_ambiguous_error(error: &Self::Error) -> bool;
}

fn parse_mode(value: Option<&str>) -> Result<Option<ParseMode>, TelegramApiError> {
    value.map(ParseMode::try_from).transpose().map_err(|_| TelegramApiError::InvalidParseMode)
}

fn caption(request: &TelegramPublicationRequest) -> String {
    request.caption.clone().unwrap_or_default()
}

#[async_trait]
impl TelegramPublicationApi for TeloxideApi {
    type Error = TelegramApiError;

    async fn copy_from_storage(
        &self,
        request: &TelegramPublicationRequest,
    ) -> Result<i64, Self::Error> {
        let message_id = i32::try_from(request.storage_message_id)
            .map_err(|_| TelegramApiError::InvalidMessageId(request.storage_message_id))?;
        let mut call = self
            .bot()
            .copy_message(
                ChatId(request.target_chat_id),
                ChatId(request.storage_chat_id),
                MessageId(message_id),
            )
            .caption(caption(request))
            .disable_notification(request.disable_notification);
        if let Some(parse_mode) = parse_mode(request.parse_mode.as_deref())? {
            call = call.parse_mode(parse_mode);
        }
        call.await.map(|message_id| i64::from(message_id.0)).map_err(TelegramApiError::Api)
    }

    async fn send_storage_file(
        &self,
        request: &TelegramPublicationRequest,
    ) -> Result<i64, Self::Error> {
        let file_id = request
            .telegram_file_id
            .as_deref()
            .filter(|file_id| !file_id.is_empty())
            .ok_or(TelegramApiError::MissingFileReference { media_kind: request.media_kind })?;
        let input = InputFile::file_id(FileId(file_id.to_owned()));
        let caption = caption(request);
        let parse_mode = parse_mode(request.parse_mode.as_deref())?;
        let message = match request.media_kind {
            MediaKind::Video => {
                let mut call = self
                    .bot()
                    .send_video(ChatId(request.target_chat_id), input)
                    .caption(caption)
                    .disable_notification(request.disable_notification)
                    .supports_streaming(true);
                if let Some(parse_mode) = parse_mode {
                    call = call.parse_mode(parse_mode);
                }
                call.await.map_err(TelegramApiError::Api)?
            }
            MediaKind::Image => {
                let mut call = self
                    .bot()
                    .send_photo(ChatId(request.target_chat_id), input)
                    .caption(caption)
                    .disable_notification(request.disable_notification);
                if let Some(parse_mode) = parse_mode {
                    call = call.parse_mode(parse_mode);
                }
                call.await.map_err(TelegramApiError::Api)?
            }
            MediaKind::Audio => {
                let mut call = self
                    .bot()
                    .send_audio(ChatId(request.target_chat_id), input)
                    .caption(caption)
                    .disable_notification(request.disable_notification);
                if let Some(parse_mode) = parse_mode {
                    call = call.parse_mode(parse_mode);
                }
                call.await.map_err(TelegramApiError::Api)?
            }
            MediaKind::Animation => {
                let mut call = self
                    .bot()
                    .send_animation(ChatId(request.target_chat_id), input)
                    .caption(caption)
                    .disable_notification(request.disable_notification);
                if let Some(parse_mode) = parse_mode {
                    call = call.parse_mode(parse_mode);
                }
                call.await.map_err(TelegramApiError::Api)?
            }
        };
        Ok(i64::from(message.id.0))
    }

    fn is_copy_unavailable(error: &Self::Error) -> bool {
        match error {
            TelegramApiError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::MessageToCopyNotFound,
            )) => true,
            TelegramApiError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::Unknown(message),
            )) => {
                let message = message.to_ascii_lowercase();
                message.contains("can't copy messages of this type")
                    || message.contains("cannot copy messages of this type")
                    || message.contains("message to copy not found")
            }
            _ => false,
        }
    }

    fn is_known_caption_error(error: &Self::Error) -> bool {
        matches!(
            error,
            TelegramApiError::Api(teloxide::RequestError::Api(
                teloxide::errors::ApiError::CantParseEntities(_)
                    | teloxide::errors::ApiError::MessageIsTooLong
                    | teloxide::errors::ApiError::MessageTextIsEmpty,
            ))
        )
    }

    fn is_retryable_no_effect(error: &Self::Error) -> bool {
        matches!(error, TelegramApiError::Api(teloxide::RequestError::RetryAfter(_)))
    }

    fn is_ambiguous_error(error: &Self::Error) -> bool {
        matches!(
            error,
            TelegramApiError::Api(
                teloxide::RequestError::Network(_)
                    | teloxide::RequestError::InvalidJson { .. }
                    | teloxide::RequestError::Io(_)
                    | teloxide::RequestError::Api(teloxide::errors::ApiError::Unknown(_)),
            )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_copy_rejections_allow_file_id_fallback() {
        let missing = TelegramApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::MessageToCopyNotFound,
        ));
        let unsupported = TelegramApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::Unknown(
                "Bad Request: can't copy messages of this type".to_owned(),
            ),
        ));
        let unknown = TelegramApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::Unknown("Bad Request: something else".to_owned()),
        ));

        assert!(TeloxideApi::is_copy_unavailable(&missing));
        assert!(TeloxideApi::is_copy_unavailable(&unsupported));
        assert!(!TeloxideApi::is_copy_unavailable(&unknown));
    }

    #[test]
    fn caption_parse_errors_are_known_but_unknown_api_errors_are_ambiguous() {
        let caption_error = TelegramApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::CantParseEntities(
                "Bad Request: can't parse entities".to_owned(),
            ),
        ));
        let unknown_error = TelegramApiError::Api(teloxide::RequestError::Api(
            teloxide::errors::ApiError::Unknown("Bad Request: unknown".to_owned()),
        ));

        assert!(TeloxideApi::is_known_caption_error(&caption_error));
        assert!(!TeloxideApi::is_ambiguous_error(&caption_error));
        assert!(TeloxideApi::is_ambiguous_error(&unknown_error));
    }

    #[test]
    fn missing_public_caption_is_encoded_as_an_empty_caption() {
        let request = TelegramPublicationRequest {
            target_chat_id: -100,
            storage_chat_id: -200,
            storage_message_id: 1,
            telegram_file_id: Some("file".to_owned()),
            media_kind: MediaKind::Video,
            caption: None,
            parse_mode: None,
            disable_notification: false,
        };
        assert_eq!(caption(&request), "");
    }
}
