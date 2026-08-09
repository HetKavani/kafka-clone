export interface LiveStreamMessage {
  timestamp: string;
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: any;
}

export class KafkaXStream {
  private ws: WebSocket | null = null;
  private onMessageCallback: ((message: LiveStreamMessage) => void) | null = null;
  private url: string;
  private isConnecting: boolean = false;

  constructor() {
    const apiURL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
    this.url = apiURL.replace(/^http/, 'ws') + '/api/v1/events/stream';
  }

  connect(onMessage: (message: LiveStreamMessage) => void) {
    if (this.ws || this.isConnecting) return;
    this.isConnecting = true;
    this.onMessageCallback = onMessage;

    try {
      this.ws = new WebSocket(this.url);
      
      this.ws.onopen = () => {
        this.isConnecting = false;
        console.log('WebSocket stream connection established.');
      };

      this.ws.onmessage = (event) => {
        if (this.onMessageCallback) {
          try {
            const data = JSON.parse(event.data);
            this.onMessageCallback(data);
          } catch (e) {
            console.error('Failed to parse websocket frame:', e);
          }
        }
      };

      this.ws.onerror = (err) => {
        this.isConnecting = false;
        // Suppress warning since the endpoint is not yet implemented on the backend
        console.warn('WebSocket stream connection error (WebSocket endpoint is prepared but not active on the backend):', err);
      };

      this.ws.onclose = () => {
        this.isConnecting = false;
        this.ws = null;
        console.log('WebSocket stream connection closed.');
      };
    } catch (e) {
      this.isConnecting = false;
      console.warn('Failed to start WebSocket client:', e);
    }
  }

  disconnect() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}
