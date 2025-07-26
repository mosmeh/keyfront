use crate::{
    backend::{Backend, COMPATIBLE_VERSION},
    client::{Client, CommandError, ConnectionError},
};
use bstr::ByteSlice;
use keyfront::{ByteBuf, commands::ClientCommand, resp::WriteResp, string::parse_int};

#[expect(clippy::unnecessary_wraps)]
impl Client<'_> {
    pub(super) fn quit(&mut self) -> Result<(), CommandError> {
        self.reply.write_ok();
        Err(ConnectionError::Quit.into())
    }

    pub(super) fn reset(&mut self) -> Result<(), CommandError> {
        self.reply.write_simple("RESET");
        Ok(())
    }

    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    pub(super) fn hello(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let mut opts = args;
        if let [proto_ver, rest @ ..] = opts {
            match parse_int::<i64>(proto_ver) {
                Some(2) => {}
                Some(_) => {
                    self.reply
                        .write_error("-NOPROTO unsupported protocol version");
                    return Ok(());
                }
                None => {
                    self.reply
                        .write_error("-ERR Protocol version is not an integer or out of range");
                    return Ok(());
                }
            }
            opts = rest;
        }

        let mut client_name = None;
        loop {
            match opts {
                [] => break,
                [opt, _user, _password, ..] if opt.eq_ignore_ascii_case(b"AUTH") => {
                    return Err(CommandError::Unimplemented);
                }
                [opt, name, rest @ ..] if opt.eq_ignore_ascii_case(b"SETNAME") => {
                    match validate_client_name(name) {
                        Ok(name) => client_name = Some(name),
                        Err(e) => {
                            self.reply.write_error(e.to_string());
                            return Ok(());
                        }
                    }
                    opts = rest;
                }
                [opt, ..] => {
                    write!(
                        self.reply,
                        "-ERR Syntax error in HELLO option '{}'",
                        opt.as_bstr()
                    );
                    return Ok(());
                }
            }
        }

        if let Some(name) = client_name {
            self.name = name.cloned();
        }

        self.reply.write_array(14);

        self.reply.write_bulk("server");
        self.reply.write_bulk("redis");

        self.reply.write_bulk("version");
        self.reply.write_bulk(
            self.server
                .backend
                .as_ref()
                .map_or(COMPATIBLE_VERSION, Backend::version),
        );

        self.reply.write_bulk("proto");
        self.reply.write_integer(2);

        self.reply.write_bulk("id");
        self.reply.write_integer(self.id);

        self.reply.write_bulk("mode");
        self.reply.write_bulk("cluster");

        self.reply.write_bulk("role");
        self.reply.write_bulk("master");

        self.reply.write_bulk("modules");
        self.reply.write_array(0);

        Ok(())
    }

    // SELECT index
    pub(super) fn select(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [index] = args else { unreachable!() };
        match parse_int::<i32>(index) {
            Some(0) => {
                self.reply.write_ok();
                Ok(())
            }
            Some(_) => {
                self.reply
                    .write_error("-ERR SELECT is not allowed in cluster mode");
                Ok(())
            }
            None => Err(CommandError::InvalidInteger),
        }
    }

    pub(super) fn client(
        &mut self,
        command: ClientCommand,
        args: &[ByteBuf],
    ) -> Result<(), CommandError> {
        match command {
            ClientCommand::Id => self.client_id(),
            ClientCommand::Getname => self.client_getname(),
            ClientCommand::Setname => self.client_setname(args),
            _ => Err(CommandError::Unimplemented),
        }
    }

    fn client_id(&mut self) -> Result<(), CommandError> {
        self.reply.write_integer(self.id);
        Ok(())
    }

    fn client_getname(&mut self) -> Result<(), CommandError> {
        if let Some(name) = &self.name {
            self.reply.write_bulk(name);
        } else {
            self.reply.write_null();
        }
        Ok(())
    }

    // CLIENT SETNAME connection-name
    fn client_setname(&mut self, args: &[ByteBuf]) -> Result<(), CommandError> {
        let [name] = args else { unreachable!() };
        match validate_client_name(name) {
            Ok(name) => {
                self.name = name.cloned();
                self.reply.write_ok();
            }
            Err(e) => self.reply.write_error(e.to_string()),
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum ValidateClientNameError {
    #[error("-ERR Client names cannot contain spaces, newlines or special characters.")]
    InvalidCharacters,
}

fn validate_client_name<T: AsRef<[u8]>>(name: &T) -> Result<Option<&T>, ValidateClientNameError> {
    match name.as_ref() {
        [] => Ok(None),
        n if n.iter().all(u8::is_ascii_graphic) => Ok(Some(name)),
        _ => Err(ValidateClientNameError::InvalidCharacters),
    }
}
