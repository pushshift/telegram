--
-- PostgreSQL database dump
--

-- Dumped from database version 11.5 (Ubuntu 11.5-0ubuntu0.19.04.1)
-- Dumped by pg_dump version 11.5 (Ubuntu 11.5-0ubuntu0.19.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: channel; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.channel (
    id bigint NOT NULL,
    name text,
    retrieved_utc integer,
    updated_utc integer,
    min_message_id integer,
    max_message_id integer,
    is_active boolean DEFAULT true,
    is_complete boolean DEFAULT false
);


ALTER TABLE public.channel OWNER TO postgres;

--
-- Name: channel_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.channel_data (
    id bigint NOT NULL,
    updated_utc integer,
    data jsonb
);


ALTER TABLE public.channel_data OWNER TO postgres;

--
-- Name: channel_status; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.channel_status (
    channel_id bigint NOT NULL,
    min_message_id integer,
    max_message_id integer,
    complete_history boolean,
    updated_utc integer
);


ALTER TABLE public.channel_status OWNER TO postgres;

--
-- Name: TABLE channel_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.channel_status IS 'Keep track of ingest location for channel';


--
-- Name: message; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.message (
    id bigint NOT NULL,
    message_id integer,
    channel_id bigint,
    retrieved_utc integer,
    updated_utc integer,
    data jsonb
);


ALTER TABLE public.message OWNER TO postgres;

--
-- Name: TABLE message; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.message IS 'raw data for each ingested message';


--
-- Name: channel_data channel_data_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.channel_data
    ADD CONSTRAINT channel_data_pkey PRIMARY KEY (id);


--
-- Name: channel channel_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.channel
    ADD CONSTRAINT channel_pkey PRIMARY KEY (id);


--
-- Name: channel_status channel_status_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.channel_status
    ADD CONSTRAINT channel_status_pkey PRIMARY KEY (channel_id);


--
-- Name: message message_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.message
    ADD CONSTRAINT message_pkey PRIMARY KEY (id);


--
-- Name: channel_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX channel_id_idx ON public.message USING btree (channel_id);


--
-- Name: channel_name_lower_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX channel_name_lower_idx ON public.channel USING btree (lower(name));


--
-- Name: message_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX message_id_idx ON public.message USING btree (message_id, channel_id);


--
-- Name: updated_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX updated_idx ON public.message USING btree (updated_utc);


--
-- PostgreSQL database dump complete
--

